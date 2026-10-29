from pyspark import pipelines as dp

import transforms

BRONZE_SILVER = spark.conf.get("bronze_silver_schema")


@dp.materialized_view(
    name="gold_study_documents",
    comment="PII-free study documents for RAG / Vector Search (allow-listed columns only)",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
@dp.expect_all_or_drop(transforms.GOLD_RULES)
def gold_study_documents():
    return transforms.documents(spark.read.table(f"{BRONZE_SILVER}.silver_studies"))
