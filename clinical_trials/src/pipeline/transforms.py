"""DataFrame transforms for the clinical trials pipeline.

Plain functions (DataFrame in, DataFrame out) so they can be unit-tested with local
PySpark. bronze.py / silver.py / gold.py wrap them in Spark Declarative Pipeline datasets.
"""

from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------


def bronze_from_binary(files: DataFrame) -> DataFrame:
    """Project binaryFile rows (path, modificationTime, length, content) to bronze columns.

    Invalid UTF-8 bytes become U+FFFD instead of failing, so no file is ever dropped.
    """
    return files.select(
        F.expr("make_valid_utf8(CAST(content AS STRING))").alias("raw_json"),
        F.sha2(F.col("content"), 256).alias("file_sha256"),
        F.col("path").alias("source_file"),
        F.col("length").alias("file_bytes"),
        F.col("modificationTime").alias("file_modified_at"),
        F.current_timestamp().alias("bronze_loaded_at"),
    )


# ---------------------------------------------------------------------------
# Silver: parsing
# ---------------------------------------------------------------------------

KNOWN_OVERALL_STATUSES = (
    "ACTIVE_NOT_RECRUITING", "APPROVED_FOR_MARKETING", "AVAILABLE", "COMPLETED",
    "ENROLLING_BY_INVITATION", "NO_LONGER_AVAILABLE", "NOT_YET_RECRUITING", "RECRUITING",
    "SUSPENDED", "TEMPORARILY_NOT_AVAILABLE", "TERMINATED", "UNKNOWN", "WITHDRAWN", "WITHHELD",
)


def _null_is_failure(rules):
    """Wrap each rule so a NULL result counts as a failure."""
    return {name: f"coalesce(({expr}), false)" for name, expr in rules.items()}


# Critical rules: failing rows are dropped from validated and land in quarantine.
CRITICAL_RULES = _null_is_failure({
    "valid_json": "is_valid_json",
    "http_status_200": "http_status = 200",
    "nct_id_present": "nct_id IS NOT NULL",
    "nct_id_format": "nct_id RLIKE '^NCT[0-9]{8}$'",
    "nct_id_matches_request": "nct_id = requested_nct_id",
    "title_present": "trim(brief_title) <> ''",
    "overall_status_present": "overall_status IS NOT NULL",
    "last_update_date_present": "last_update_posted_date IS NOT NULL",
})

# Warning rules: rows are kept; failures are tracked in DQ metrics and dq_warnings.
WARNING_RULES = _null_is_failure({
    "known_overall_status": "overall_status IN ("
    + ", ".join(f"'{s}'" for s in KNOWN_OVERALL_STATUSES) + ")",
    "enrollment_non_negative": "enrollment_count >= 0",
    "completion_after_start": "completion_date >= start_date",
    "primary_completion_after_start": "primary_completion_date >= start_date",
    "start_date_parses": "start_date IS NOT NULL",
    "min_age_not_above_max_age": "minimum_age_years <= maximum_age_years",
})


def _failed(rules) -> Column:
    """Array of the names of the rules a row fails."""
    return F.array_compact(F.array(*[F.when(~F.expr(e), F.lit(n)) for n, e in rules.items()]))


def _get(path: str, type_: str) -> Column:
    """Read one field of the API response; a missing or mistyped field gives NULL."""
    return F.try_variant_get(F.col("payload"), f"$.{path}", type_)


def _date(path: str) -> Column:
    """ClinicalTrials.gov dates are yyyy-MM-dd or yyyy-MM (month precision → first of month)."""
    raw = _get(path, "string")
    return F.coalesce(
        F.try_to_timestamp(raw, F.lit("yyyy-MM-dd")),
        F.try_to_timestamp(raw, F.lit("yyyy-MM")),
    ).cast("date")


_UNIT_TO_YEARS = {"year": 1.0, "month": 12.0, "week": 52.1775, "day": 365.25,
                  "hour": 8766.0, "minute": 525960.0}


def _age_in_years(raw: Column) -> Column:
    """'18 Years' → 18.0, '6 Months' → 0.5; 'N/A' or unknown units → NULL."""
    number = F.regexp_extract(raw, r"^\s*(\d+(?:\.\d+)?)", 1).try_cast("double")
    unit = F.lower(F.regexp_extract(raw, r"([A-Za-z]+)\s*$", 1))
    divisor = None
    for name, value in _UNIT_TO_YEARS.items():
        cond = unit.startswith(name)
        divisor = F.when(cond, value) if divisor is None else divisor.when(cond, value)
    return F.round(number / divisor, 3)


PROTO = "protocolSection"
INTERVENTIONS_TYPE = "array<struct<type:string,name:string>>"
OUTCOMES_TYPE = "array<struct<measure:string,timeFrame:string>>"


def parse_studies(bronze: DataFrame) -> DataFrame:
    """Parse bronze envelopes into typed study columns plus failed_rules / dq_warnings.

    A search-page envelope (payload.studies) becomes one row per study; any other envelope (a
    single study, an error body, unparseable JSON) is one row. Every field is read with
    try_parse_json / try_variant_get, so schema drift yields NULLs (and rule failures) rather
    than errors. Used by both validated and quarantine.
    """
    env = bronze.withColumn("_env", F.try_parse_json("raw_json"))
    page_studies = F.try_variant_get("_env", "$.payload.studies", "array<variant>")
    # Only small columns go through the explode: a 1,000-study page is ~12 MB, and carrying
    # raw_json or _env would copy it onto every study row.
    meta = env.select(
        F.col("_env").isNotNull().alias("is_valid_json"),
        # Single-study envelopes name the study they asked for; fall back to the landing folder
        # ({NCT_ID}/...) when the file isn't valid JSON.
        F.coalesce(
            F.try_variant_get("_env", "$.requested_nct_id", "string"),
            F.nullif(F.regexp_extract("source_file", r"(NCT[0-9]{8})[/\\][^/\\]+$", 1), F.lit("")),
        ).alias("_envelope_nct_id"),
        F.try_variant_get("_env", "$.source_url", "string").alias("source_url"),
        F.try_variant_get("_env", "$.http_status", "int").alias("http_status"),
        F.try_variant_get("_env", "$.ingested_at", "string").try_cast("timestamp").alias("ingested_at"),
        F.try_variant_get("_env", "$.ingest_id", "string").alias("ingest_id"),
        "source_file",
        "file_sha256",
        "file_modified_at",
        "bronze_loaded_at",
        F.substring("raw_json", 1, 2000).alias("raw_json_snippet"),
        # An empty page yields no rows; it isn't an error.
        F.explode(F.coalesce(
            page_studies, F.array(F.try_variant_get("_env", "$.payload", "variant")))).alias("payload"),
    )

    interventions = _get(f"{PROTO}.armsInterventionsModule.interventions", INTERVENTIONS_TYPE)
    primary_outcomes = _get(f"{PROTO}.outcomesModule.primaryOutcomes", OUTCOMES_TYPE)
    secondary_outcomes = _get(f"{PROTO}.outcomesModule.secondaryOutcomes", OUTCOMES_TYPE)
    minimum_age = _get(f"{PROTO}.eligibilityModule.minimumAge", "string")
    maximum_age = _get(f"{PROTO}.eligibilityModule.maximumAge", "string")
    party = f"{PROTO}.sponsorCollaboratorsModule.responsibleParty"

    parsed = meta.select(
        _get(f"{PROTO}.identificationModule.nctId", "string").alias("nct_id"),
        # A search page asks for no particular study, so each study answers for itself.
        F.coalesce("_envelope_nct_id", _get(f"{PROTO}.identificationModule.nctId", "string"))
        .alias("requested_nct_id"),
        "http_status",
        "is_valid_json",
        _get(f"{PROTO}.identificationModule.briefTitle", "string").alias("brief_title"),
        _get(f"{PROTO}.identificationModule.officialTitle", "string").alias("official_title"),
        _get(f"{PROTO}.identificationModule.acronym", "string").alias("acronym"),
        _get(f"{PROTO}.identificationModule.organization.fullName", "string").alias("organization_name"),
        _get(f"{PROTO}.identificationModule.organization.class", "string").alias("organization_class"),
        _get(f"{PROTO}.statusModule.overallStatus", "string").alias("overall_status"),
        _date(f"{PROTO}.statusModule.startDateStruct.date").alias("start_date"),
        _date(f"{PROTO}.statusModule.primaryCompletionDateStruct.date").alias("primary_completion_date"),
        _date(f"{PROTO}.statusModule.completionDateStruct.date").alias("completion_date"),
        _date(f"{PROTO}.statusModule.studyFirstPostDateStruct.date").alias("study_first_posted_date"),
        _date(f"{PROTO}.statusModule.lastUpdatePostDateStruct.date").alias("last_update_posted_date"),
        _get(f"{PROTO}.sponsorCollaboratorsModule.leadSponsor.name", "string").alias("lead_sponsor_name"),
        _get(f"{PROTO}.sponsorCollaboratorsModule.leadSponsor.class", "string").alias("lead_sponsor_class"),
        _get(f"{party}.type", "string").alias("responsible_party_type"),
        _get(f"{party}.investigatorFullName", "string").alias("responsible_party_investigator_name"),
        _get(f"{party}.investigatorTitle", "string").alias("responsible_party_investigator_title"),
        _get(f"{party}.investigatorAffiliation", "string").alias("responsible_party_investigator_affiliation"),
        _get(f"{PROTO}.designModule.studyType", "string").alias("study_type"),
        _get(f"{PROTO}.designModule.phases", "array<string>").alias("phases"),
        _get(f"{PROTO}.designModule.enrollmentInfo.count", "int").alias("enrollment_count"),
        _get(f"{PROTO}.designModule.enrollmentInfo.type", "string").alias("enrollment_type"),
        _get(f"{PROTO}.conditionsModule.conditions", "array<string>").alias("conditions"),
        _get(f"{PROTO}.conditionsModule.keywords", "array<string>").alias("keywords"),
        F.transform(interventions, lambda i: i["name"]).alias("intervention_names"),
        F.array_sort(F.array_distinct(F.transform(interventions, lambda i: i["type"]))).alias("intervention_types"),
        F.transform(primary_outcomes, lambda o: o["measure"]).alias("primary_outcome_measures"),
        F.transform(secondary_outcomes, lambda o: o["measure"]).alias("secondary_outcome_measures"),
        _get(f"{PROTO}.descriptionModule.briefSummary", "string").alias("brief_summary"),
        _get(f"{PROTO}.descriptionModule.detailedDescription", "string").alias("detailed_description"),
        _get(f"{PROTO}.eligibilityModule.eligibilityCriteria", "string").alias("eligibility_criteria"),
        _get(f"{PROTO}.eligibilityModule.sex", "string").alias("sex"),
        minimum_age.alias("minimum_age"),
        _age_in_years(minimum_age).alias("minimum_age_years"),
        maximum_age.alias("maximum_age"),
        _age_in_years(maximum_age).alias("maximum_age_years"),
        _get(f"{PROTO}.eligibilityModule.healthyVolunteers", "boolean").alias("healthy_volunteers"),
        _get("hasResults", "boolean").alias("has_results"),
        F.sha2(F.to_json("payload"), 256).alias("payload_hash"),
        "ingest_id",
        "ingested_at",
        "source_url",
        "source_file",
        "file_sha256",
        "file_modified_at",
        "bronze_loaded_at",
        "raw_json_snippet",
        "payload",
    )
    return parsed.withColumns({
        "failed_rules": _failed(CRITICAL_RULES),
        "dq_warnings": _failed(WARNING_RULES),
    })


def validated(parsed: DataFrame) -> DataFrame:
    """Columns of silver_studies_validated (the critical-rule drop is applied by expectations)."""
    return parsed.drop("failed_rules", "raw_json_snippet")


def quarantine(parsed: DataFrame) -> DataFrame:
    """Records failing at least one critical rule, with a raw JSON snippet for triage."""
    return parsed.where(F.size("failed_rules") > 0).select(
        "failed_rules",
        "dq_warnings",
        "requested_nct_id",
        "nct_id",
        "http_status",
        "brief_title",
        "ingest_id",
        "ingested_at",
        "source_url",
        "source_file",
        "file_sha256",
        "bronze_loaded_at",
        "raw_json_snippet",
        F.current_timestamp().alias("quarantined_at"),
    )


def apply_critical_rules(parsed: DataFrame) -> DataFrame:
    """What expect_all_or_drop(CRITICAL_RULES) does in the pipeline; used by local tests."""
    return parsed.where(F.size("failed_rules") == 0)


# ---------------------------------------------------------------------------
# Silver: current version per study, contacts, locations
# ---------------------------------------------------------------------------


# silver_studies is an Auto CDC (SCD Type 1) target fed by silver_studies_validated: one row per
# nct_id, the newest last_update_posted_date winning, then the newest ingested_at.
CDC_KEYS = ["nct_id"]
CDC_SEQUENCE = ["last_update_posted_date", "ingested_at"]


def apply_scd1(changes: DataFrame) -> DataFrame:
    """What the SCD Type 1 Auto CDC flow keeps (newest row per key); used by local tests."""
    newest_first = Window.partitionBy(*CDC_KEYS).orderBy(
        *[F.col(c).desc_nulls_last() for c in CDC_SEQUENCE])
    return changes.withColumn("_rank", F.row_number().over(newest_first)).where("_rank = 1").drop("_rank")


PERSON_TYPE = "array<struct<name:string,role:string,phone:string,phoneExt:string,email:string,affiliation:string>>"
LOCATIONS_TYPE = (
    "array<struct<facility:string,status:string,city:string,state:string,zip:string,country:string,"
    "geoPoint:struct<lat:double,lon:double>,"
    "contacts:array<struct<name:string,role:string,phone:string,phoneExt:string,email:string>>>>"
)
CONTACTS = f"{PROTO}.contactsLocationsModule"


def _blank_to_null(c: Column) -> Column:
    return F.when(F.trim(c) != "", F.trim(c))


def _people(latest: DataFrame, source: str, path: str) -> DataFrame:
    person = F.explode(_get(path, PERSON_TYPE)).alias("p")
    return latest.select("nct_id", person).select(
        "nct_id",
        F.lit(source).alias("source"),
        "p.role", "p.name", "p.email", "p.phone", "p.phoneExt", "p.affiliation",
        F.lit(None).cast("string").alias("facility"),
    )


def contacts(latest: DataFrame) -> DataFrame:
    """One row per person per study from every place the API lists people."""
    central = _people(latest, "central_contact", f"{CONTACTS}.centralContacts")
    officials = _people(latest, "overall_official", f"{CONTACTS}.overallOfficials")
    site_contacts = (
        latest.select("nct_id", F.explode(_get(f"{CONTACTS}.locations", LOCATIONS_TYPE)).alias("l"))
        .select("nct_id", "l.facility", F.explode("l.contacts").alias("p"))
        .select(
            "nct_id",
            F.lit("location_contact").alias("source"),
            "p.role", "p.name", "p.email", "p.phone", "p.phoneExt",
            F.lit(None).cast("string").alias("affiliation"),
            "facility",
        )
    )
    responsible = latest.where(F.col("responsible_party_investigator_name").isNotNull()).select(
        "nct_id",
        F.lit("responsible_party").alias("source"),
        F.col("responsible_party_investigator_title").alias("role"),
        F.col("responsible_party_investigator_name").alias("name"),
        F.lit(None).cast("string").alias("email"),
        F.lit(None).cast("string").alias("phone"),
        F.lit(None).cast("string").alias("phoneExt"),
        F.col("responsible_party_investigator_affiliation").alias("affiliation"),
        F.lit(None).cast("string").alias("facility"),
    )

    people = (
        central.unionByName(officials).unionByName(site_contacts).unionByName(responsible)
        .select(
            "nct_id",
            "source",
            _blank_to_null(F.col("role")).alias("role"),
            _blank_to_null(F.regexp_replace("name", r"\s+", " ")).alias("name"),
            _blank_to_null(F.lower("email")).alias("email"),
            _blank_to_null(F.regexp_replace("phone", r"[^0-9+]", "")).alias("phone"),
            _blank_to_null(F.regexp_replace("phoneExt", r"[^0-9]", "")).alias("phone_ext"),
            _blank_to_null(F.col("affiliation")).alias("affiliation"),
            _blank_to_null(F.col("facility")).alias("facility"),
        )
        .withColumn("person_key", F.coalesce(F.lower("name"), "email", "phone"))
        .where(F.col("person_key").isNotNull())
    )
    return (
        people.groupBy("nct_id", "person_key")
        .agg(
            F.max("name").alias("name"),
            F.max("email").alias("email"),
            F.max("phone").alias("phone"),
            F.max("phone_ext").alias("phone_ext"),
            F.max("affiliation").alias("affiliation"),
            F.array_sort(F.collect_set("role")).alias("roles"),
            F.array_sort(F.collect_set("source")).alias("sources"),
            F.array_sort(F.collect_set("facility")).alias("facilities"),
        )
        .select(
            F.sha2(F.concat_ws("|", "nct_id", "person_key"), 256).alias("contact_id"),
            "nct_id", "name", "email", "phone", "phone_ext", "affiliation", "roles", "sources", "facilities",
        )
    )


LOCATION_RULES = _null_is_failure({
    "country_present": "trim(country) <> ''",
    "valid_coordinates": "lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180",
})


def locations(latest: DataFrame) -> DataFrame:
    """One row per study site (site contacts live in contacts, not here)."""
    sites = latest.select(
        "nct_id", F.explode(_get(f"{CONTACTS}.locations", LOCATIONS_TYPE)).alias("l")
    ).select(
        "nct_id",
        _blank_to_null(F.col("l.facility")).alias("facility"),
        _blank_to_null(F.col("l.status")).alias("status"),
        _blank_to_null(F.col("l.city")).alias("city"),
        _blank_to_null(F.col("l.state")).alias("state"),
        _blank_to_null(F.col("l.zip")).alias("zip"),
        _blank_to_null(F.col("l.country")).alias("country"),
        F.col("l.geoPoint.lat").alias("lat"),
        F.col("l.geoPoint.lon").alias("lon"),
    )
    key = F.concat_ws("|", *[F.lower(F.coalesce(F.col(c), F.lit(""))) for c in
                             ("nct_id", "facility", "city", "state", "zip", "country")])
    return (
        sites.withColumn("location_id", F.sha2(key, 256))
        .dropDuplicates(["location_id"])
        .select("location_id", *sites.columns)
    )


# ---------------------------------------------------------------------------
# Silver: eligibility criteria and outcomes
# ---------------------------------------------------------------------------

# A section header line in eligibilityCriteria: "Inclusion Criteria:", "**Key Exclusion Criteria**",
# "EXCLUSION CRITERIA (Cohort 2):". Group 1 is the section, group 2 any text after the colon.
CRITERIA_HEADER = (
    r"(?i)^[\s*#_>-]*(?:key\s+|main\s+|major\s+|general\s+)?(inclusion|exclusion)\s+criteri(?:a|on)"
    r"(?:\s*\([^)]*\)|\s+for\s+[^:]{1,60})?[\s*_]*(?::|$)[\s*_]*(.*)$"
)
# A list item: "* text", "- text", "1. text", "a) text". Group 1 is the indent, group 2 the text.
CRITERIA_BULLET = r"^(\s*)(?:[*•+-]|\d{1,3}[.)]|[a-zA-Z][.)])\s+(.*)$"


def eligibility_criteria(latest: DataFrame) -> DataFrame:
    """One row per inclusion / exclusion criterion, split out of the eligibilityCriteria text.

    Each top-level list item is one criterion; its sub-items and wrapped lines are joined onto it.
    A section without list items gives one criterion per non-blank line. Text before the first
    section header has criterion_type 'unspecified'.
    """
    by_line = Window.partitionBy("nct_id").orderBy("line_no")
    # ClinicalTrials.gov markdown escapes punctuation ("\>= 18 years"); unescape it first.
    text = F.regexp_replace("eligibility_criteria", r"\\([^\w\s])", "$1")
    lines = (
        latest.select("nct_id", F.posexplode(F.split(text, r"\r?\n")).alias("line_no", "line"))
        .withColumn("_header", F.lower(F.regexp_extract("line", CRITERIA_HEADER, 1)))
        .withColumn("_content", F.when(F.col("_header") != "",
                                       F.regexp_extract("line", CRITERIA_HEADER, 2)).otherwise(F.col("line")))
        .withColumn("criterion_type", F.coalesce(
            F.last(F.when(F.col("_header") != "", F.col("_header")), ignorenulls=True).over(
                by_line.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
            F.lit("unspecified")))
        .withColumn("_is_bullet", F.col("_content").rlike(CRITERIA_BULLET))
        .withColumn("_indent", F.when(
            F.col("_is_bullet"), F.length(F.regexp_extract("_content", CRITERIA_BULLET, 1))))
        .withColumn("_text", F.trim(F.when(F.col("_is_bullet"), F.regexp_extract("_content", CRITERIA_BULLET, 2))
                                    .otherwise(F.col("_content"))))
        .where(F.col("_text") != "")
    )
    section = Window.partitionBy("nct_id", "criterion_type")
    # A top-level item starts at the least-indented list marker; with no markers, every line does.
    starts = F.when(F.col("_top_indent").isNull(), True).otherwise(
        F.coalesce(F.col("_indent") <= F.col("_top_indent"), F.lit(False)))
    items = (
        lines.withColumn("_top_indent", F.min("_indent").over(section))
        .withColumn("_start", starts.cast("int"))
        .withColumn("_item", F.sum("_start").over(
            section.orderBy("line_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        .groupBy("nct_id", "criterion_type", "_item")
        .agg(F.min("line_no").alias("_first_line"),
             F.array_sort(F.collect_list(F.struct("line_no", "_text"))).alias("_lines"))
        .select("nct_id", "criterion_type", "_first_line",
                F.regexp_replace(F.array_join(F.transform("_lines", lambda x: x["_text"]), " "),
                                 r"\s+", " ").alias("criterion_text"))
        .withColumn("criterion_number", F.row_number().over(
            Window.partitionBy("nct_id", "criterion_type").orderBy("_first_line")))
    )
    return items.select(
        F.sha2(F.concat_ws("|", "nct_id", "criterion_type", "criterion_number"), 256).alias("criterion_id"),
        "nct_id", "criterion_type", "criterion_number", "criterion_text",
    )


def conditions(latest: DataFrame) -> DataFrame:
    """One row per condition per study, in the order the study lists them; blanks and
    case-insensitive repeats within a study are dropped."""
    exploded = latest.select("nct_id", F.posexplode("conditions").alias("pos", "condition")).select(
        "nct_id", "pos", _blank_to_null(F.regexp_replace("condition", r"\s+", " ")).alias("condition"),
    ).where(F.col("condition").isNotNull())
    first_seen = Window.partitionBy("nct_id", F.lower("condition")).orderBy("pos")
    by_position = Window.partitionBy("nct_id").orderBy("pos")
    return (
        exploded.withColumn("_rank", F.row_number().over(first_seen)).where("_rank = 1")
        .withColumn("condition_number", F.row_number().over(by_position))
        .select(
            F.sha2(F.concat_ws("|", "nct_id", F.lower("condition")), 256).alias("condition_id"),
            "nct_id", "condition_number", "condition",
        )
    )


OUTCOME_DETAIL_TYPE ="array<struct<measure:string,description:string,timeFrame:string>>"
OUTCOME_KINDS = (("primary", "primaryOutcomes"), ("secondary", "secondaryOutcomes"), ("other", "otherOutcomes"))


def outcomes(latest: DataFrame) -> DataFrame:
    """One row per planned outcome measure (primary, secondary, other pre-specified)."""
    parts = [
        latest.select("nct_id", F.posexplode(_get(f"{PROTO}.outcomesModule.{field}", OUTCOME_DETAIL_TYPE))
                      .alias("pos", "o"))
        .select(
            "nct_id",
            F.lit(kind).alias("outcome_type"),
            (F.col("pos") + 1).alias("outcome_number"),
            _blank_to_null(F.col("o.measure")).alias("measure"),
            _blank_to_null(F.col("o.description")).alias("description"),
            _blank_to_null(F.col("o.timeFrame")).alias("time_frame"),
        )
        for kind, field in OUTCOME_KINDS
    ]
    all_outcomes = reduce(DataFrame.unionByName, parts).where(F.col("measure").isNotNull())
    return all_outcomes.select(
        F.sha2(F.concat_ws("|", "nct_id", "outcome_type", "outcome_number"), 256).alias("outcome_id"),
        *all_outcomes.columns,
    )


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------

EMAIL_PATTERN = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
# 10+ digit North-American-style or international numbers. Dates (2018-05-15, 2018-05,
# 05/15/2018) never have the 3-3-4 digit grouping, and the look-arounds stop matches inside
# longer tokens such as NCT IDs or hashes.
PHONE_PATTERN = r"(?<![\w-])(?:\+?\d{1,3}[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}(?![\w-])"
MIN_DOCUMENT_CHARS = 200

# Explicit allow-list: no contacts, no responsible-party fields, no raw payload.
GOLD_COLUMNS = (
    "nct_id", "brief_title", "official_title", "acronym", "organization_name", "organization_class",
    "overall_status", "start_date", "primary_completion_date", "completion_date",
    "study_first_posted_date", "last_update_posted_date", "lead_sponsor_name", "lead_sponsor_class",
    "study_type", "phases", "enrollment_count", "enrollment_type", "conditions", "keywords",
    "intervention_names", "intervention_types", "primary_outcome_measures",
    "secondary_outcome_measures", "brief_summary", "detailed_description", "eligibility_criteria",
    "sex", "minimum_age", "minimum_age_years", "maximum_age", "maximum_age_years",
    "healthy_volunteers", "has_results", "payload_hash",
)
FREE_TEXT_COLUMNS = ("brief_title", "official_title", "brief_summary", "detailed_description",
                     "eligibility_criteria")
FREE_TEXT_ARRAY_COLUMNS = ("primary_outcome_measures", "secondary_outcome_measures")


def scrub(text: Column) -> Column:
    """Replace emails and phone-shaped numbers; dates are left intact."""
    return F.regexp_replace(
        F.regexp_replace(text, EMAIL_PATTERN, "[email removed]"), PHONE_PATTERN, "[phone removed]"
    )


def _line(label: str, value: Column) -> Column:
    """'Label: value', or NULL (skipped by concat_ws) when the value is missing."""
    return F.concat(F.lit(f"{label}: "), value)


def _joined(c: str) -> Column:
    return F.when(F.size(c) > 0, F.array_join(c, "; "))


def documents(silver_studies: DataFrame) -> DataFrame:
    """PII-free study documents for RAG: allow-listed columns + labelled document_text."""
    docs = silver_studies.select(*GOLD_COLUMNS)
    docs = docs.withColumns({c: scrub(F.col(c)) for c in FREE_TEXT_COLUMNS})
    docs = docs.withColumns({c: F.transform(c, scrub) for c in FREE_TEXT_ARRAY_COLUMNS})

    dates = F.concat_ws(
        "; ",
        _line("start", F.col("start_date").cast("string")),
        _line("primary completion", F.col("primary_completion_date").cast("string")),
        _line("completion", F.col("completion_date").cast("string")),
        _line("first posted", F.col("study_first_posted_date").cast("string")),
        _line("last updated", F.col("last_update_posted_date").cast("string")),
    )
    enrollment = F.concat_ws(
        " ", F.col("enrollment_count").cast("string"), F.lower("enrollment_type")
    )
    eligibility = F.concat_ws(
        "; ",
        _line("sex", F.col("sex")),
        _line("minimum age", F.col("minimum_age")),
        _line("maximum age", F.col("maximum_age")),
        _line("healthy volunteers", F.col("healthy_volunteers").cast("string")),
    )
    text = F.concat_ws(
        "\n",
        _line("Study", F.concat_ws(" - ", "nct_id", "brief_title")),
        _line("Official title", F.col("official_title")),
        _line("Status", F.col("overall_status")),
        _line("Study type", F.col("study_type")),
        _line("Phase", _joined("phases")),
        _line("Conditions", _joined("conditions")),
        _line("Interventions", _joined("intervention_names")),
        _line("Sponsor", F.col("lead_sponsor_name")),
        _line("Enrollment", F.when(enrollment != "", enrollment)),
        _line("Dates", F.when(dates != "", dates)),
        _line("Primary outcomes", _joined("primary_outcome_measures")),
        _line("Summary", F.col("brief_summary")),
        _line("Description", F.col("detailed_description")),
        _line("Eligibility", F.when(eligibility != "", eligibility)),
        _line("Eligibility criteria", F.col("eligibility_criteria")),
    )
    return docs.withColumn("document_text", scrub(text)).withColumn(
        "document_chars", F.length("document_text")
    )


GOLD_RULES = _null_is_failure({"document_long_enough": f"document_chars >= {MIN_DOCUMENT_CHARS}"})


# ---------------------------------------------------------------------------
# Gold: chronic lymphocytic leukemia (CLL) / Waldenström macroglobulinemia (WM) cohort
# ---------------------------------------------------------------------------

# (term, disease group, pattern), run on lower-cased conditions, keywords and titles. Covers
# spelling variants, MeSH-style inverted names ("Leukemia, Lymphocytic, Chronic, B-Cell") and
# acronyms. SLL and Richter transformation count as CLL, lymphoplasmacytic lymphoma as WM;
# matched_terms keeps them apart. Not matched: prolymphocytic, acute lymphocytic and T-cell
# "chronic lymphocytic" leukemia (T-PLL), and Waldenström hypergammaglobulinemia.
COHORT_TERMS = (
    ("CLL", "CLL",
     r"(?<!t[\s-]cell )chronic\s+(?:b[\s-]*(?:cell\s+)?)?lymph(?:ocytic|ocyte|oid|oma)\s+(?:b[\s-]*)?leu[ck]"
     r"|lymphocytic\s+leu[ck]\w*,?\s+chronic"
     r"|leu[ck]\w*,?\s+(?:lymphoma,?\s+)?(?:chronic\s+lymphocytic|lymphocytic,?\s+chronic)"
     r"|\b(?:b-?)?cll\b"),
    ("SLL", "CLL",
     r"small\s+lymphocytic\s+(?:lymphoma|leu[ck])|(?:lymphoma|leu[ck]\w*),?\s+small\s+lymphocytic|\bsll\b"),
    ("Richter transformation", "CLL", r"richter"),
    ("WM", "WM", r"waldenstr(?:o|ö|oe)m(?!\W*hypergamma)|macroglobulina?emia"),
    ("LPL", "WM", r"lymphoplasmac\w*\s+lymphoma|immunocytoma"),
)
# A bare "WM" only counts in conditions and keywords; in a title it can mean e.g. white matter.
WM_ACRONYM = r"\bwm\b"
COHORT_FIELDS_FOR_ACRONYM = ("condition", "keyword")


def _cohort_hits(text: Column, field: Column) -> Column:
    """array<struct<term, disease_group>> of the cohort terms the text matches."""
    lowered = F.lower(text)
    hits = [F.when(lowered.rlike(p), F.struct(F.lit(term).alias("term"), F.lit(group).alias("disease_group")))
            for term, group, p in COHORT_TERMS]
    hits.append(F.when(field.isin(*COHORT_FIELDS_FOR_ACRONYM) & lowered.rlike(WM_ACRONYM),
                       F.struct(F.lit("WM").alias("term"), F.lit("WM").alias("disease_group"))))
    return F.array_compact(F.array(*hits))


def _tagged(field: str, array_col: str) -> Column:
    return F.transform(F.coalesce(F.col(array_col), F.array().cast("array<string>")),
                       lambda v: F.struct(F.lit(field).alias("field"), v.alias("text")))


def cohort_matches(docs: DataFrame) -> DataFrame:
    """One row per study that mentions CLL or WM in its conditions, keywords or titles."""
    texts = F.concat(
        _tagged("condition", "conditions"),
        _tagged("keyword", "keywords"),
        F.array(F.struct(F.lit("title").alias("field"), F.col("brief_title").alias("text")),
                F.struct(F.lit("title").alias("field"), F.col("official_title").alias("text"))),
    )
    hits = (
        docs.select("nct_id", F.explode(texts).alias("t"))
        .where(F.col("t.text").isNotNull())
        .select("nct_id", F.col("t.field").alias("field"),
                F.explode(_cohort_hits(F.col("t.text"), F.col("t.field"))).alias("h"))
    )
    return hits.groupBy("nct_id").agg(
        F.array_sort(F.collect_set("h.disease_group")).alias("disease_groups"),
        F.array_sort(F.collect_set("h.term")).alias("matched_terms"),
        F.array_sort(F.collect_set("field")).alias("matched_on"),
    )


def cohort_studies(docs: DataFrame) -> DataFrame:
    """gold_study_documents rows for the cohort, with what matched and where."""
    return docs.join(cohort_matches(docs), "nct_id")


def cohort_conditions(conditions: DataFrame, cohort: DataFrame) -> DataFrame:
    """Conditions of cohort studies; disease_groups / matched_terms say which ones are CLL or WM."""
    hits = _cohort_hits(F.col("condition"), F.lit("condition"))
    return conditions.join(cohort.select("nct_id"), "nct_id", "left_semi").select(
        "condition_id", "nct_id", "condition_number", "condition",
        F.array_sort(F.array_distinct(F.transform(hits, lambda h: h["disease_group"]))).alias("disease_groups"),
        F.array_sort(F.array_distinct(F.transform(hits, lambda h: h["term"]))).alias("matched_terms"),
    )


def cohort_eligibility_criteria(criteria: DataFrame, cohort: DataFrame) -> DataFrame:
    """Eligibility criteria of cohort studies, scrubbed of emails and phone numbers."""
    return criteria.join(cohort.select("nct_id"), "nct_id", "left_semi").select(
        "criterion_id", "nct_id", "criterion_type", "criterion_number",
        scrub(F.col("criterion_text")).alias("criterion_text"),
    )


def cohort_outcomes(outcomes_df: DataFrame, cohort: DataFrame) -> DataFrame:
    """Planned outcome measures of cohort studies, scrubbed of emails and phone numbers."""
    return outcomes_df.join(cohort.select("nct_id"), "nct_id", "left_semi").select(
        "outcome_id", "nct_id", "outcome_type", "outcome_number",
        *[scrub(F.col(c)).alias(c) for c in ("measure", "description", "time_frame")],
    )


# ---------------------------------------------------------------------------
# Gold: CLL trial features for the completion model (src/06_train_cll_trial_model.py)
# ---------------------------------------------------------------------------

# Label: did a finished trial end early? COMPLETED → 0, TERMINATED / WITHDRAWN → 1, anything
# else (ongoing, UNKNOWN) → NULL, i.e. not trained on; ongoing trials are scored instead.
ENDED_EARLY_STATUSES = ("TERMINATED", "WITHDRAWN")
ONGOING_STATUSES = ("RECRUITING", "ACTIVE_NOT_RECRUITING", "NOT_YET_RECRUITING",
                    "ENROLLING_BY_INVITATION", "SUSPENDED")

# (feature, pattern) run on lower-cased intervention names + other names. Generic, brand and
# code names of the main CLL treatment classes.
DRUG_CLASSES = (
    ("btk_inhibitor", r"ibrutinib|acalabrutinib|zanubrutinib|pirtobrutinib|orelabrutinib|nemtabrutinib"
                      r"|tirabrutinib|imbruvica|calquence|brukinsa|jaypirca|pci-32765|acp-196|bgb-3111|loxo-305"),
    ("bcl2_inhibitor", r"venetoclax|venclexta|abt-199|gdc-0199|sonrotoclax|bgb-11417|lisaftoclax|apg-2575"
                       r"|navitoclax|abt-263"),
    ("anti_cd20", r"rituximab|rituxan|mabthera|obinutuzumab|gazyva|ga101|ofatumumab|arzerra|ublituximab"
                  r"|veltuzumab"),
    ("pi3k_inhibitor", r"idelalisib|zydelig|cal-101|duvelisib|copiktra|ipi-145|umbralisib|copanlisib"
                       r"|parsaclisib"),
    ("chemotherapy", r"fludarabine|cyclophosphamide|bendamustine|chlorambucil|pentostatin|cladribine"
                     r"|doxorubicin|vincristine|cytarabine|oxaliplatin|cisplatin|etoposide|\bfcr\b"),
    ("anti_cd52", r"alemtuzumab|campath"),
    ("immunomodulator", r"lenalidomide|revlimid|thalidomide|pomalidomide"),
    ("cell_therapy", r"car[\s-]?t\b|chimeric antigen|lisocabtagene|liso-cel|tisagenlecleucel|axicabtagene"
                     r"|\bnk cells?\b|donor lymphocyte"),
    ("stem_cell_transplant", r"transplant|\bhsct\b|\bhct\b|stem cell"),
    ("bispecific_antibody", r"epcoritamab|glofitamab|mosunetuzumab|odronextamab|blinatumomab|bispecific"),
    ("corticosteroid", r"prednison|methylprednisolone|dexamethasone"),
    ("vaccine", r"vaccin"),
)
INTERVENTION_TYPE_FLAGS = ("DRUG", "BIOLOGICAL", "PROCEDURE", "RADIATION", "BEHAVIORAL", "DEVICE",
                           "DIETARY_SUPPLEMENT", "GENETIC", "OTHER")
DESIGN = f"{PROTO}.designModule.designInfo"
OTHER_NAMES_TYPE = "array<struct<name:string,otherNames:array<string>>>"


def _category(c: Column) -> Column:
    """Categorical feature: missing or blank → 'UNKNOWN', booleans as 'true' / 'false'."""
    s = F.trim(c.cast("string"))
    return F.when(s.isNull() | (s == ""), F.lit("UNKNOWN")).otherwise(F.upper(s))


def trial_features(cohort: DataFrame, studies: DataFrame, criteria: DataFrame) -> DataFrame:
    """One row per cohort study: design-time features and the ended_early label.

    Features known when a trial is registered only. Left out because they record what happened
    to the trial: enrollment (ACTUAL counts), results, completion dates and site lists (often
    emptied when a trial is withdrawn).
    """
    interventions = _get(f"{PROTO}.armsInterventionsModule.interventions", OTHER_NAMES_TYPE)
    design = studies.select(
        "nct_id",
        F.lower(F.concat_ws(" | ", F.flatten(F.transform(
            F.coalesce(interventions, F.array()),
            lambda i: F.concat(F.array(i["name"]), F.coalesce(i["otherNames"], F.array()))
        )))).alias("_intervention_text"),
        _category(_get(f"{DESIGN}.allocation", "string")).alias("allocation"),
        _category(_get(f"{DESIGN}.interventionModel", "string")).alias("intervention_model"),
        _category(_get(f"{DESIGN}.primaryPurpose", "string")).alias("primary_purpose"),
        _category(_get(f"{DESIGN}.maskingInfo.masking", "string")).alias("masking"),
        _category(_get(f"{PROTO}.oversightModule.oversightHasDmc", "boolean")).alias("has_dmc"),
        _category(_get(f"{PROTO}.oversightModule.isFdaRegulatedDrug", "boolean")).alias("fda_regulated_drug"),
        F.coalesce(F.size(_get(f"{PROTO}.sponsorCollaboratorsModule.collaborators", "array<variant>")),
                   F.lit(0)).alias("n_collaborators"),
        F.coalesce(F.size(_get(f"{PROTO}.armsInterventionsModule.armGroups", "array<variant>")),
                   F.lit(0)).alias("n_arms"),
    )
    counts = criteria.groupBy("nct_id").agg(
        F.count(F.when(F.col("criterion_type") == "inclusion", 1)).alias("n_inclusion_criteria"),
        F.count(F.when(F.col("criterion_type") == "exclusion", 1)).alias("n_exclusion_criteria"),
        F.count(F.when(F.col("criterion_type") == "unspecified", 1)).alias("n_unspecified_criteria"),
    )
    status = F.col("overall_status")
    types = F.coalesce(F.col("intervention_types"), F.array())
    df = cohort.join(design, "nct_id", "left").join(counts, "nct_id", "left")
    flags = {c: F.col("_intervention_text").rlike(p) for c, p in DRUG_CLASSES}
    return df.select(
        "nct_id", "brief_title", "overall_status",
        F.when(status == "COMPLETED", 0).when(status.isin(*ENDED_EARLY_STATUSES), 1).alias("ended_early"),
        F.coalesce(status.isin(*ONGOING_STATUSES), F.lit(False)).alias("is_ongoing"),
        F.year("start_date").alias("start_year"),
        _category(F.array_join(F.array_sort("phases"), "/")).alias("phase"),
        _category(F.col("study_type")).alias("study_type"),
        _category(F.col("lead_sponsor_class")).alias("lead_sponsor_class"),
        "allocation", "intervention_model", "primary_purpose", "masking", "has_dmc", "fda_regulated_drug",
        "n_collaborators", "n_arms",
        F.coalesce(F.size("intervention_names"), F.lit(0)).alias("n_interventions"),
        *[F.array_contains(types, t).alias(f"has_{t.lower()}_intervention") for t in INTERVENTION_TYPE_FLAGS],
        *[F.coalesce(v, F.lit(False)).alias(c) for c, v in flags.items()],
        reduce(lambda a, b: a + b, [F.coalesce(v, F.lit(False)).cast("int") for v in flags.values()])
        .alias("n_drug_classes"),
        F.array_contains(F.col("disease_groups"), "WM").alias("also_wm"),
        F.coalesce(F.size("conditions"), F.lit(0)).alias("n_conditions"),
        F.coalesce(F.size("primary_outcome_measures"), F.lit(0)).alias("n_primary_outcomes"),
        F.coalesce(F.size("secondary_outcome_measures"), F.lit(0)).alias("n_secondary_outcomes"),
        *[F.coalesce(c, F.lit(0)).alias(c) for c in
          ("n_inclusion_criteria", "n_exclusion_criteria", "n_unspecified_criteria")],
        "minimum_age_years", "maximum_age_years",
        _category(F.col("sex")).alias("sex"),
        _category(F.col("healthy_volunteers")).alias("healthy_volunteers"),
    )
