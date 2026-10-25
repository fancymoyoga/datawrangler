# Databricks notebook source
# MAGIC %md
# MAGIC # Load landing JSON files with Auto Loader
# MAGIC
# MAGIC Reads the clinical_trials landing folder incrementally with Auto Loader (`cloudFiles`) and
# MAGIC appends new files to two bronze tables. Each run processes only files it hasn't seen
# MAGIC before (`availableNow`), then stops.
# MAGIC
# MAGIC | Stream | Files | Table (one row per) |
# MAGIC |---|---|---|
# MAGIC | `study_files` | `{source_path}/NCT*/*.json` | `study_files_raw` (file) |
# MAGIC | `search_pages` | `{source_path}/search/*.json` | `search_studies_raw` (study in a page) |
# MAGIC
# MAGIC Every file is one envelope: `requested_nct_id`, `source_url`, `http_status`, `ingested_at`,
# MAGIC `ingest_id`, `payload`. `payload` is inferred as a nested struct.
# MAGIC
# MAGIC **Schema evolution:** when a file brings a field the inferred schema doesn't have, at any
# MAGIC depth, Auto Loader adds it to the schema in `schemaLocation` and stops the stream
# MAGIC (`addNewColumns`). `write_table` restarts the stream, which picks up the new schema, and
# MAGIC `mergeSchema` adds the column to the Delta table. A value whose type doesn't match its
# MAGIC column lands in `_rescued_data` instead.
# MAGIC
# MAGIC Parameters:
# MAGIC - `catalog`, `schema`: where the tables are written
# MAGIC - `source_path`: landing folder, e.g. `/Volumes/workspace/<schema>/landing/studies`
# MAGIC - `checkpoint_root`: volume folder for checkpoints and inferred schemas

# COMMAND ----------

from pyspark.sql import functions as F

from streaming import run_until_done

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path").rstrip("/")
checkpoint_root = dbutils.widgets.get("checkpoint_root").rstrip("/")

# Envelope column types; everything else, payload included, is inferred.
SCHEMA_HINTS = "http_status INT, ingested_at TIMESTAMP"

# COMMAND ----------


def read_envelopes(path: str, glob: str, stream: str):
    """Auto Loader stream of landing envelopes under `path`, plus file lineage columns."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_root}/{stream}/schema")
        .option("cloudFiles.schemaHints", SCHEMA_HINTS)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("pathGlobFilter", glob)
        .load(path)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("source_file_modified", F.col("_metadata.file_modification_time"))
        .withColumn("loaded_at", F.current_timestamp())
    )


def write_table(build_df, table: str, stream: str):
    """Append everything new since the last run to `table`, then return.

    `build_df` makes the stream's DataFrame. It's called again after each schema change, so the
    restarted stream reads with the evolved schema.
    """
    run_until_done(
        lambda: build_df()
        .writeStream.option("checkpointLocation", f"{checkpoint_root}/{stream}/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.{schema}.{table}"),
        table,
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-study files: one row per file

# COMMAND ----------

def study_files():
    # Study folders are NCT<id>/; the glob keeps search/ pages out of this stream.
    return read_envelopes(f"{source_path}/NCT*", "*.json", "study_files").withColumn(
        "nct_id",
        F.coalesce(
            F.col("payload.protocolSection.identificationModule.nctId"),
            F.col("requested_nct_id"),
        ),
    )


write_table(study_files, "study_files_raw", "study_files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Search pages: one row per study in a page
# MAGIC
# MAGIC A page's `payload` is `{"totalCount": n, "studies": [...]}`; each study becomes a `study`
# MAGIC struct. A failed page (no `studies`) lands no rows.

# COMMAND ----------


def search_studies():
    return (
        read_envelopes(f"{source_path}/search", "*.json", "search_pages")
        .withColumn("total_count", F.col("payload.totalCount"))
        .select("*", F.posexplode("payload.studies").alias("page_position", "study"))
        .drop("payload")
        .withColumn("nct_id", F.col("study.protocolSection.identificationModule.nctId"))
    )


write_table(search_studies, "search_studies_raw", "search_pages")
