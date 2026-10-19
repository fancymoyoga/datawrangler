from pyspark import pipelines as dp

import transforms


@dp.materialized_view(
    name="gold_study_documents",
    comment="PII-free study documents for RAG / Vector Search (allow-listed columns only)",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
@dp.expect_all_or_drop(transforms.GOLD_RULES)
def gold_study_documents():
    return transforms.documents(spark.read.table("silver_studies"))
