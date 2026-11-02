from pyspark import pipelines as dp
from pyspark.sql.functions import struct

import transforms

# Bronze and silver publish to their own catalog.schema; gold stays in the pipeline default.
BRONZE_SILVER = spark.conf.get("bronze_silver_schema")


@dp.temporary_view(name="silver_studies_parsed")
def silver_studies_parsed():
    # Shared by validated and quarantine, so both always see the same parse and rule results.
    return transforms.parse_studies(spark.readStream.table(f"{BRONZE_SILVER}.bronze_studies_raw"))


@dp.table(
    name=f"{BRONZE_SILVER}.silver_studies_validated",
    comment="Every study version that passed the critical rules; raw payload kept as VARIANT",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
@dp.expect_all_or_drop(transforms.CRITICAL_RULES)
@dp.expect_all(transforms.WARNING_RULES)
def silver_studies_validated():
    return transforms.validated(spark.readStream.table("silver_studies_parsed"))


@dp.table(
    name=f"{BRONZE_SILVER}.silver_studies_quarantine",
    comment="Envelopes that failed at least one critical rule, with the rules they failed",
)
def silver_studies_quarantine():
    return transforms.quarantine(spark.readStream.table("silver_studies_parsed"))


# SCD Type 1: each new validated version upserts its study, so runs only process new rows.
dp.create_streaming_table(
    name=f"{BRONZE_SILVER}.silver_studies",
    comment="Current version of each study (Auto CDC SCD Type 1: newest last update, then newest pull)",
)
dp.create_auto_cdc_flow(
    target=f"{BRONZE_SILVER}.silver_studies",
    source=f"{BRONZE_SILVER}.silver_studies_validated",
    keys=transforms.CDC_KEYS,
    sequence_by=struct(*transforms.CDC_SEQUENCE),
    stored_as_scd_type=1,
)


@dp.materialized_view(
    name=f"{BRONZE_SILVER}.silver_study_contacts",
    comment="PII: one row per person per study (central contacts, officials, site contacts, responsible party)",
)
def silver_study_contacts():
    return transforms.contacts(spark.read.table(f"{BRONZE_SILVER}.silver_studies"))


@dp.materialized_view(
    name=f"{BRONZE_SILVER}.silver_study_locations",
    comment="One row per study site",
)
@dp.expect_all(transforms.LOCATION_RULES)
def silver_study_locations():
    return transforms.locations(spark.read.table(f"{BRONZE_SILVER}.silver_studies"))


@dp.materialized_view(
    name=f"{BRONZE_SILVER}.silver_study_eligibility_criteria",
    comment="One row per inclusion / exclusion criterion, parsed from the eligibility criteria text",
)
def silver_study_eligibility_criteria():
    return transforms.eligibility_criteria(spark.read.table(f"{BRONZE_SILVER}.silver_studies"))


@dp.materialized_view(
    name=f"{BRONZE_SILVER}.silver_study_conditions",
    comment="One row per condition per study",
)
def silver_study_conditions():
    return transforms.conditions(spark.read.table(f"{BRONZE_SILVER}.silver_studies"))


@dp.materialized_view(
    name=f"{BRONZE_SILVER}.silver_study_outcomes",
    comment="One row per planned outcome measure (primary, secondary, other)",
)
def silver_study_outcomes():
    return transforms.outcomes(spark.read.table(f"{BRONZE_SILVER}.silver_studies"))
