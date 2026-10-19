# Databricks notebook source
# MAGIC %md
# MAGIC # Vector Search index on gold_study_documents
# MAGIC
# MAGIC Creates (if missing) a Vector Search endpoint and a Delta Sync index with managed
# MAGIC embeddings on `document_text`, then syncs it and waits until it's ready.
# MAGIC
# MAGIC This workspace can't index materialized views, so `gold_study_documents` (an MV) is first
# MAGIC merged into the plain Delta table `gold_study_documents_vs_source` (same columns, CDF on),
# MAGIC and the index syncs from that copy. Silver is never a source.

# COMMAND ----------

import json
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    EndpointType,
    PipelineType,
    VectorIndexType,
)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "clinical_trials")
dbutils.widgets.text("vs_endpoint", "clinical-trials-vs")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VS_ENDPOINT = dbutils.widgets.get("vs_endpoint")
EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint")

GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_study_documents"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.gold_study_documents_vs_source"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.gold_study_documents_index"
COLUMNS_TO_SYNC = ["nct_id", "brief_title", "overall_status", "study_type", "lead_sponsor_name",
                   "document_text"]
TIMEOUT_SECONDS = 40 * 60

w = WorkspaceClient()


def wait_for(what, check, timeout=TIMEOUT_SECONDS, every=30):
    deadline = time.time() + timeout
    while True:
        done, state = check()
        print(f"{what}: {state}")
        if done:
            return state
        if time.time() > deadline:
            raise TimeoutError(f"{what} not ready after {timeout}s (last state: {state})")
        time.sleep(every)


# COMMAND ----------

# MAGIC %md ## Copy gold into a plain Delta table the index can sync from

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SOURCE_TABLE}
    COMMENT 'Copy of gold_study_documents for Vector Search (which cannot sync from an MV here)'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    AS SELECT * FROM {GOLD_TABLE} WHERE false
""")
merged = spark.sql(f"""
    MERGE INTO {SOURCE_TABLE} AS t
    USING {GOLD_TABLE} AS s
    ON t.nct_id = s.nct_id
    WHEN MATCHED AND (t.payload_hash <> s.payload_hash OR t.document_text <> s.document_text)
      THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
""").first().asDict()
print(f"merged {GOLD_TABLE} -> {SOURCE_TABLE}: {merged}")

# COMMAND ----------

try:
    w.vector_search_endpoints.get_endpoint(VS_ENDPOINT)
    print(f"endpoint {VS_ENDPOINT} exists")
except NotFound:
    print(f"creating endpoint {VS_ENDPOINT}")
    w.vector_search_endpoints.create_endpoint(name=VS_ENDPOINT, endpoint_type=EndpointType.STANDARD)


def endpoint_state():
    status = w.vector_search_endpoints.get_endpoint(VS_ENDPOINT).endpoint_status
    state = status.state.value if status and status.state else "UNKNOWN"
    if state not in ("ONLINE", "PROVISIONING"):
        raise RuntimeError(f"endpoint {VS_ENDPOINT} is {state}: {status.message if status else ''}")
    return state == "ONLINE", state


wait_for(f"endpoint {VS_ENDPOINT}", endpoint_state)

# COMMAND ----------

try:
    existing = w.vector_search_indexes.get_index(INDEX_NAME)
    source = existing.delta_sync_index_spec.source_table if existing.delta_sync_index_spec else None
    if source != SOURCE_TABLE:
        raise RuntimeError(f"{INDEX_NAME} syncs from {source}, expected {SOURCE_TABLE}")
    print(f"index {INDEX_NAME} exists; triggering sync")
    w.vector_search_indexes.sync_index(INDEX_NAME)
except NotFound:
    print(f"creating index {INDEX_NAME} from {SOURCE_TABLE}")
    w.vector_search_indexes.create_index(
        name=INDEX_NAME,
        endpoint_name=VS_ENDPOINT,
        primary_key="nct_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=SOURCE_TABLE,
            pipeline_type=PipelineType.TRIGGERED,
            columns_to_sync=COLUMNS_TO_SYNC,
            embedding_source_columns=[EmbeddingSourceColumn(
                name="document_text", embedding_model_endpoint_name=EMBEDDING_ENDPOINT)],
        ),
    )


def index_state():
    status = w.vector_search_indexes.get_index(INDEX_NAME).status
    rows = status.indexed_row_count if status else None
    return bool(status and status.ready), f"ready={status.ready if status else None} rows={rows} {status.message if status else ''}"


wait_for(f"index {INDEX_NAME}", index_state)

# COMMAND ----------

expected = spark.table(GOLD_TABLE).count()
indexed = w.vector_search_indexes.get_index(INDEX_NAME).status.indexed_row_count
probe = w.vector_search_indexes.query_index(
    index_name=INDEX_NAME, columns=["nct_id", "brief_title"], query_text="clinical trial", num_results=3)
summary = {"index": INDEX_NAME, "endpoint": VS_ENDPOINT, "source_table": SOURCE_TABLE,
           "gold_rows": expected, "indexed_rows": indexed, "merge": merged,
           "probe": probe.result.data_array if probe.result else []}
print(summary)
dbutils.notebook.exit(json.dumps(summary, default=str))
