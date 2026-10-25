# Databricks notebook source
# MAGIC %md
# MAGIC # Flatten nested study fields into tables
# MAGIC
# MAGIC Streams new rows from both bronze tables and, per micro-batch, appends to:
# MAGIC - `studies`: one row per study snapshot. Every non-array field under `protocolSection` is a
# MAGIC   column, named by its snake_case path without the `Module` suffix, e.g.
# MAGIC   `statusModule.startDateStruct.date` → `status_start_date_struct_date`.
# MAGIC - `study_<array>`: one table per array in `protocolSection`, one row per element, e.g.
# MAGIC   `contactsLocationsModule.locations` → `study_locations`. Struct elements are flattened the
# MAGIC   same way; arrays inside an element stay arrays.
# MAGIC
# MAGIC Every table carries `nct_id`, `source`, `ingest_id`, `ingested_at` and `source_file`. Each
# MAGIC bronze snapshot of a study lands again, so pick the latest `ingested_at` per `nct_id`.
# MAGIC
# MAGIC Tables come from the batch's schema, so a new field becomes a column (`mergeSchema`) and a
# MAGIC new array becomes a table. When a bronze table gains columns, the stream restarts.
# MAGIC
# MAGIC Parameters: `catalog`, `schema` (bronze tables and outputs), `checkpoint_root`.

# COMMAND ----------

import re

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType

from streaming import run_until_done

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checkpoint_root = dbutils.widgets.get("checkpoint_root").rstrip("/")

KEYS = ["nct_id", "source", "ingest_id", "ingested_at", "source_file"]

# Bronze table -> column holding the study JSON.
SOURCES = {"study_files_raw": "payload", "search_studies_raw": "study"}

# COMMAND ----------


def snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).replace(".", "_").lower()


def column_name(path: list[str]) -> str:
    return "_".join(snake(re.sub(r"Module$", "", part)) for part in path)


def col(base: str, path: list[str]):
    return F.col(".".join([base, *(f"`{part}`" for part in path)]))


def flat_columns(struct: StructType, base: str, path: list[str], keep_arrays: bool):
    """Columns for every leaf under `base`, nested structs expanded."""
    for field in struct.fields:
        field_path = [*path, field.name]
        if isinstance(field.dataType, StructType):
            yield from flat_columns(field.dataType, base, field_path, keep_arrays)
        elif keep_arrays or not isinstance(field.dataType, ArrayType):
            yield col(base, field_path).alias(column_name(field_path))


def arrays(struct: StructType, path: list[str] = ()):
    """(path, array type) of every array not inside another array."""
    for field in struct.fields:
        field_path = [*path, field.name]
        if isinstance(field.dataType, ArrayType):
            yield field_path, field.dataType
        elif isinstance(field.dataType, StructType):
            yield from arrays(field.dataType, field_path)


def append(df, table: str, source: str, batch_id: int):
    # txnAppId/txnVersion make a retried batch a no-op for tables it already wrote.
    (
        df.write.mode("append")
        .option("mergeSchema", "true")
        .option("txnAppId", f"flatten_{source}_{table}")
        .option("txnVersion", batch_id)
        .saveAsTable(f"{catalog}.{schema}.{table}")
    )


def flatten_batch(batch_df, batch_id: int, source: str):
    protocol = batch_df.schema["protocolSection"].dataType
    keys = [F.col(k) for k in KEYS]

    has_results = ["hasResults AS has_results"] if "hasResults" in batch_df.columns else []
    studies = batch_df.select(
        *keys,
        *[F.expr(c) for c in has_results],
        *flat_columns(protocol, "protocolSection", [], keep_arrays=False),
    )
    append(studies, "studies", source, batch_id)

    for path, array_type in arrays(protocol):
        table = f"study_{snake(path[-1])}"
        exploded = batch_df.select(
            *keys, F.posexplode(col("protocolSection", path)).alias("position", "value")
        )
        element = array_type.elementType
        if isinstance(element, StructType):
            values = list(flat_columns(element, "value", [], keep_arrays=True))
        else:
            values = [F.col("value")]
        append(exploded.select(*keys, "position", *values), table, source, batch_id)


# COMMAND ----------


def start_flatten(source_table: str, study_column: str):
    def start():
        stream = (
            spark.readStream.table(f"{catalog}.{schema}.{source_table}")
            .select(
                "nct_id",
                F.lit(source_table.removesuffix("_raw")).alias("source"),
                "ingest_id",
                "ingested_at",
                "source_file",
                f"{study_column}.*",
            )
            .where("protocolSection IS NOT NULL")
        )
        return (
            stream.writeStream.option(
                "checkpointLocation", f"{checkpoint_root}/flatten/{source_table}/checkpoint"
            )
            .foreachBatch(lambda df, batch_id: flatten_batch(df, batch_id, source_table))
            .trigger(availableNow=True)
            .start()
        )

    return start


for source_table, study_column in SOURCES.items():
    run_until_done(start_flatten(source_table, study_column), f"flatten {source_table}")
