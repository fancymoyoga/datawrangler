# Databricks notebook source
# MAGIC %md
# MAGIC # Tag PII with Unity Catalog column tags
# MAGIC
# MAGIC Scans every table in the schema and sets column tags `pii` (`true` / `quasi` / `embedded`),
# MAGIC `pii_type` and `pii_detection`, plus a table tag `contains_pii`. Every decision is appended
# MAGIC to `pii_tag_audit`.
# MAGIC
# MAGIC Detection passes, first match wins:
# MAGIC 1. column-name rules
# MAGIC 2. regex over up to 1000 sampled values (emails, phone numbers; date-shaped values excluded)
# MAGIC 3. optional `ai_classify` over up to 20 short distinct values
# MAGIC
# MAGIC The run fails if a direct identifier is found in `gold_study_documents` by passes 1 or 2;
# MAGIC `ai_classify`-only hits there only warn.

# COMMAND ----------

import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from pipeline.transforms import EMAIL_PATTERN, PHONE_PATTERN

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "clinical_trials")
dbutils.widgets.dropdown("use_ai_pii_check", "true", ["true", "false"])

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
USE_AI = dbutils.widgets.get("use_ai_pii_check") == "true"

AUDIT_TABLE = "pii_tag_audit"
GOLD_TABLES = {"gold_study_documents", "gold_study_documents_vs_source"}
DIRECT_IDENTIFIERS = {"email", "phone", "person_name", "raw_json"}
SAMPLE_ROWS = 1000
AI_SAMPLE_VALUES = 20
AI_MAX_VALUE_CHARS = 100
AI_MIN_HIT_SHARE = 0.5
REGEX_DIRECT_SHARE = 0.8  # share of matching values at which a column *is* the identifier
DATE_SHAPED = r"^\s*\d{4}-\d{2}(-\d{2})?([ T][0-9:.]+Z?)?\s*$|^\s*\d{1,2}/\d{1,2}/\d{2,4}\s*$"

RUN_ID = str(uuid.uuid4())
RUN_AT = datetime.now(timezone.utc)


def fq(table):
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


ALTER_KIND = {"MATERIALIZED_VIEW": "MATERIALIZED VIEW", "STREAMING_TABLE": "STREAMING TABLE"}

# COMMAND ----------

# MAGIC %md ## Inventory

# COMMAND ----------

tables = spark.sql(f"""
    SELECT table_name, table_type
    FROM `{CATALOG}`.information_schema.tables
    WHERE table_schema = '{SCHEMA}' AND table_name <> '{AUDIT_TABLE}'
      AND table_type IN ('MANAGED', 'EXTERNAL', 'MATERIALIZED_VIEW', 'STREAMING_TABLE')
      AND table_name NOT LIKE '\\_\\_materialization\\_%'  -- pipeline-internal backing tables
    ORDER BY table_name
""").collect()

columns = spark.sql(f"""
    SELECT table_name, column_name, full_data_type
    FROM `{CATALOG}`.information_schema.columns
    WHERE table_schema = '{SCHEMA}'
    ORDER BY table_name, ordinal_position
""").collect()

table_type = {t.table_name: t.table_type for t in tables}
scan = [c for c in columns if c.table_name in table_type]
print(f"{len(table_type)} tables, {len(scan)} columns to scan in {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md ## Pass 1: column-name rules

# COMMAND ----------

NAME_RULES = [  # (pattern on lower-cased column name, pii, pii_type)
    (r"email", "true", "email"),
    (r"phone", "true", "phone"),
    (r"^name$|(investigator|contact|official|person|patient)_(full_)?name$", "true", "person_name"),
    (r"(investigator|contact)_title$", "quasi", "job_title"),  # not official_title (the study's)
    (r"(^|_)(zip|zip_code|postal_code|postcode)$", "quasi", "postal_code"),
    (r"^(lat|lon|lng|latitude|longitude)$|_(lat|lon|lng|latitude|longitude)$", "quasi", "geo_coordinate"),
    (r"^(raw_json|raw_json_snippet|payload)$", "embedded", "raw_json"),
]


def name_rule(column_name, data_type):
    name = column_name.lower()
    for pattern, pii, pii_type in NAME_RULES:
        if re.search(pattern, name):
            return pii, pii_type, f"column name matches /{pattern}/"
    if data_type.lower() == "variant":
        return "embedded", "raw_json", "VARIANT column holds raw API JSON"
    return None


decisions = {}  # (table, column) -> dict
for c in scan:
    hit = name_rule(c.column_name, c.full_data_type)
    if hit:
        pii, pii_type, evidence = hit
        decisions[(c.table_name, c.column_name)] = dict(
            detection="name_rule", pii=pii, pii_type=pii_type, evidence=evidence)

print(f"pass 1: {len(decisions)} columns")

# COMMAND ----------

# MAGIC %md ## Pass 2: regex over sampled values

# COMMAND ----------


def as_text(c):
    col = F.col(f"`{c.column_name}`")
    return F.array_join(col, " ") if c.full_data_type.lower() == "array<string>" else col


def is_text(c):
    return c.full_data_type.lower() in ("string", "array<string>")


for c in scan:
    if (c.table_name, c.column_name) in decisions or not is_text(c):
        continue
    sample = (spark.table(fq(c.table_name)).select(as_text(c).alias("v"))
              .where("v IS NOT NULL AND trim(v) <> ''").limit(SAMPLE_ROWS))
    stats = sample.agg(
        F.count("*").alias("n"),
        F.sum(F.col("v").rlike(EMAIL_PATTERN).cast("int")).alias("email"),
        F.sum((F.col("v").rlike(PHONE_PATTERN) & ~F.col("v").rlike(DATE_SHAPED)).cast("int")).alias("phone"),
    ).first()
    if not stats.n or not (stats.email or stats.phone):
        continue
    pii_type = "email" if stats.email >= stats.phone else "phone"
    share = max(stats.email, stats.phone) / stats.n
    decisions[(c.table_name, c.column_name)] = dict(
        detection="regex",
        pii="true" if share >= REGEX_DIRECT_SHARE else "embedded",
        pii_type=pii_type,
        evidence=f"{stats.email} email / {stats.phone} phone matches in {stats.n} sampled values",
    )

print(f"after pass 2: {len(decisions)} columns")

# COMMAND ----------

# MAGIC %md ## Pass 3: ai_classify (optional)

# COMMAND ----------

AI_LABELS = {"person name": ("true", "person_name"), "email address": ("true", "email"),
             "phone number": ("true", "phone"), "street address": ("quasi", "street_address")}
AI_LABEL_JSON = '["person name", "email address", "phone number", "street address", "not personal data"]'
AI_SKIP = re.compile(r"(_id|_hash|sha256|_url|_file|_date|_at)$")
ai_errors = []

if USE_AI:
    try:
        spark.sql(f"SELECT ai_classify('Jane Doe', '{AI_LABEL_JSON}', map('version', '2.0'))").collect()
    except Exception as e:
        USE_AI = False
        print(f"WARNING: ai_classify unavailable, skipping pass 3: {str(e).splitlines()[0]}")

if USE_AI:
    for c in scan:
        key = (c.table_name, c.column_name)
        if key in decisions or c.full_data_type.lower() != "string" or AI_SKIP.search(c.column_name):
            continue
        try:
            values = (spark.table(fq(c.table_name)).select(F.col(f"`{c.column_name}`").alias("v"))
                      .where(f"v IS NOT NULL AND length(v) BETWEEN 2 AND {AI_MAX_VALUE_CHARS}")
                      .distinct().limit(AI_SAMPLE_VALUES))
            labels = [r.label for r in values.selectExpr(
                f"ai_classify(v, '{AI_LABEL_JSON}', map('version', '2.0')):response[0]::STRING AS label"
            ).collect()]
        except Exception as e:
            ai_errors.append((key, str(e).splitlines()[0]))
            continue
        hits = [l for l in labels if l in AI_LABELS]
        if not labels or len(hits) < 2 or len(hits) / len(labels) < AI_MIN_HIT_SHARE:
            continue
        top = max(set(hits), key=hits.count)
        pii, pii_type = AI_LABELS[top]
        decisions[key] = dict(detection="ai_classify", pii=pii, pii_type=pii_type,
                              evidence=f"{len(hits)}/{len(labels)} sampled values classified as PII ({top})")
    for key, err in ai_errors:
        print(f"WARNING: ai_classify failed for {key}: {err}")

print(f"after pass 3: {len(decisions)} columns (ai_classify {'on' if USE_AI else 'off'})")

# COMMAND ----------

# MAGIC %md ## Apply tags and write the audit

# COMMAND ----------

audit = []
tag_errors = []


def record(table, column, data_type, action, d=None, error=None):
    d = d or {}
    audit.append((RUN_ID, RUN_AT, CATALOG, SCHEMA, table, table_type[table], column, data_type,
                  d.get("detection", "none"), d.get("pii"), d.get("pii_type"), d.get("evidence"),
                  action, error))


def alter(table, clause):
    spark.sql(f"ALTER {ALTER_KIND.get(table_type[table], 'TABLE')} {fq(table)} {clause}")


for c in scan:
    d = decisions.get((c.table_name, c.column_name))
    if not d:
        record(c.table_name, c.column_name, c.full_data_type, "no_pii")
        continue
    try:
        alter(c.table_name, f"ALTER COLUMN `{c.column_name}` SET TAGS ("
                            f"'pii' = '{d['pii']}', 'pii_type' = '{d['pii_type']}', "
                            f"'pii_detection' = '{d['detection']}')")
        record(c.table_name, c.column_name, c.full_data_type, "tagged", d)
    except Exception as e:
        tag_errors.append(f"{c.table_name}.{c.column_name}: {str(e).splitlines()[0]}")
        record(c.table_name, c.column_name, c.full_data_type, "error", d, str(e)[:1000])

for table in table_type:
    has_pii = any(t == table for t, _ in decisions)
    try:
        alter(table, f"SET TAGS ('contains_pii' = '{str(has_pii).lower()}')")
        record(table, None, None, "table_tagged", dict(detection="summary", pii=str(has_pii).lower()))
    except Exception as e:
        tag_errors.append(f"{table}: {str(e).splitlines()[0]}")
        record(table, None, None, "error", error=str(e)[:1000])

# Remove tags this run no longer confirms (rule fixes, data changes, out-of-scope tables), so the
# catalog always reflects the latest scan.
PII_TAGS = ("pii", "pii_type", "pii_detection")
stale_columns = spark.sql(f"""
    SELECT DISTINCT table_name, column_name FROM `{CATALOG}`.information_schema.column_tags
    WHERE schema_name = '{SCHEMA}' AND tag_name IN {PII_TAGS}
""").collect()
stale_tables = spark.sql(f"""
    SELECT t.table_name, i.table_type FROM `{CATALOG}`.information_schema.table_tags t
    JOIN `{CATALOG}`.information_schema.tables i
      ON i.table_schema = t.schema_name AND i.table_name = t.table_name
    WHERE t.schema_name = '{SCHEMA}' AND t.tag_name = 'contains_pii'
""").collect()
all_types = {**{r.table_name: r.table_type for r in stale_tables}, **table_type}
scanned_tables = set(table_type)
unset_tags = ", ".join(f"'{t}'" for t in PII_TAGS)

for r in stale_columns:
    if (r.table_name, r.column_name) in decisions:
        continue
    kind = ALTER_KIND.get(all_types.get(r.table_name, "MANAGED"), "TABLE")
    try:
        spark.sql(f"ALTER {kind} {fq(r.table_name)} ALTER COLUMN `{r.column_name}` UNSET TAGS ({unset_tags})")
        table_type.setdefault(r.table_name, all_types.get(r.table_name, "MANAGED"))
        record(r.table_name, r.column_name, None, "untagged", dict(evidence="no longer detected or out of scope"))
    except Exception as e:
        tag_errors.append(f"unset {r.table_name}.{r.column_name}: {str(e).splitlines()[0]}")

for r in stale_tables:
    if r.table_name in scanned_tables:
        continue  # contains_pii was just set by this run
    try:
        spark.sql(f"ALTER {ALTER_KIND.get(r.table_type, 'TABLE')} {fq(r.table_name)} UNSET TAGS ('contains_pii')")
        table_type.setdefault(r.table_name, r.table_type)
        record(r.table_name, None, None, "untagged", dict(evidence="table out of scope"))
    except Exception as e:
        tag_errors.append(f"unset {r.table_name}: {str(e).splitlines()[0]}")

AUDIT_SCHEMA = """run_id STRING, run_at TIMESTAMP, catalog STRING, schema STRING, table_name STRING,
    table_type STRING, column_name STRING, data_type STRING, pii_detection STRING, pii STRING,
    pii_type STRING, evidence STRING, action STRING, error STRING"""
spark.createDataFrame(audit, AUDIT_SCHEMA).write.mode("append").saveAsTable(fq(AUDIT_TABLE))
print(f"audit: {len(audit)} rows appended to {CATALOG}.{SCHEMA}.{AUDIT_TABLE} (run_id={RUN_ID})")

# COMMAND ----------

# MAGIC %md ## Gold gate

# COMMAND ----------

gold = {f"{t}.{col}": d for (t, col), d in decisions.items() if t in GOLD_TABLES}
gold_direct = {col: d for col, d in gold.items()
               if d["detection"] in ("name_rule", "regex") and d["pii_type"] in DIRECT_IDENTIFIERS}
for col, d in gold.items():
    if col not in gold_direct:
        print(f"WARNING: {col} flagged by {d['detection']}: {d['evidence']}")

summary = {
    "run_id": RUN_ID,
    "tables": len(scanned_tables),
    "columns_untagged": sum(1 for a in audit if a[12] == "untagged" and a[6]),
    "columns_scanned": len(scan),
    "columns_tagged": sum(1 for a in audit if a[12] == "tagged"),
    "by_detection": {k: sum(1 for d in decisions.values() if d["detection"] == k)
                     for k in ("name_rule", "regex", "ai_classify")},
    "ai_classify": USE_AI,
    "gold_flags": {col: d["detection"] for col, d in gold.items()},
    "tag_errors": tag_errors,
}
print(summary)

if gold_direct:
    raise RuntimeError("Direct identifiers found in gold: "
                       + ", ".join(f"{c} ({d['pii_type']}, {d['detection']}: {d['evidence']})"
                                   for c, d in gold_direct.items()))
if tag_errors:
    raise RuntimeError("Some tags could not be applied:\n" + "\n".join(tag_errors))

import json  # noqa: E402

dbutils.notebook.exit(json.dumps(summary))
