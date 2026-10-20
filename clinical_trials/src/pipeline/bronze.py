from pyspark import pipelines as dp

import transforms

LANDING_PATH = spark.conf.get("landing_path")


@dp.table(
    name="bronze_studies_raw",
    comment="Landed ClinicalTrials.gov envelopes, one row per file, unparsed",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
def bronze_studies_raw():
    # binaryFile reads every file as bytes, so nothing is parsed, inferred or dropped here.
    files = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.json")
        .load(LANDING_PATH)
    )
    return transforms.bronze_from_binary(files)
