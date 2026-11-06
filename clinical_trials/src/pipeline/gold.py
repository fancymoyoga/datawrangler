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


# CLL / Waldenström macroglobulinemia cohort: the studies, then their child rows. Published to the
# clinicaltrials catalog with bronze and silver; gold_study_documents stays put for Vector Search.


@dp.materialized_view(
    name=f"{BRONZE_SILVER}.gold_cll_wm_studies",
    comment="Studies about chronic lymphocytic leukemia (incl. SLL, Richter) or Waldenström "
            "macroglobulinemia (incl. LPL): gold_study_documents columns plus what matched and where",
)
def gold_cll_wm_studies():
    return transforms.cohort_studies(spark.read.table("gold_study_documents"))


@dp.materialized_view(
    name=f"{BRONZE_SILVER}.gold_cll_wm_conditions",
    comment="Conditions of the CLL / WM studies, one row per condition; matched_terms marks the CLL / WM ones",
)
def gold_cll_wm_conditions():
    return transforms.cohort_conditions(
        spark.read.table(f"{BRONZE_SILVER}.silver_study_conditions"), spark.read.table(f"{BRONZE_SILVER}.gold_cll_wm_studies"))


@dp.materialized_view(
    name=f"{BRONZE_SILVER}.gold_cll_wm_eligibility_criteria",
    comment="Inclusion / exclusion criteria of the CLL / WM studies (emails and phones scrubbed)",
)
def gold_cll_wm_eligibility_criteria():
    return transforms.cohort_eligibility_criteria(
        spark.read.table(f"{BRONZE_SILVER}.silver_study_eligibility_criteria"),
        spark.read.table(f"{BRONZE_SILVER}.gold_cll_wm_studies"))


@dp.materialized_view(
    name=f"{BRONZE_SILVER}.gold_cll_wm_outcomes",
    comment="Planned outcome measures of the CLL / WM studies (emails and phones scrubbed)",
)
def gold_cll_wm_outcomes():
    return transforms.cohort_outcomes(
        spark.read.table(f"{BRONZE_SILVER}.silver_study_outcomes"), spark.read.table(f"{BRONZE_SILVER}.gold_cll_wm_studies"))


@dp.materialized_view(
    name=f"{BRONZE_SILVER}.gold_cll_trial_features",
    comment="Design-time features of the CLL studies and the ended_early label (1 = terminated or "
            "withdrawn, 0 = completed, NULL = ongoing / unknown); training data for 06_train_cll_trial_model",
)
def gold_cll_trial_features():
    cll = spark.read.table(f"{BRONZE_SILVER}.gold_cll_wm_studies").where("array_contains(disease_groups, 'CLL')")
    return transforms.trial_features(
        cll, spark.read.table(f"{BRONZE_SILVER}.silver_studies"),
        spark.read.table(f"{BRONZE_SILVER}.silver_study_eligibility_criteria"))


@dp.materialized_view(
    name=f"{BRONZE_SILVER}.gold_cll_wm_outcome_results",
    comment="Posted outcome results of the CLL / WM studies per arm, with each arm's drug classes "
            "(free text scrubbed). Values as reported by each trial; not comparable across trials.",
)
def gold_cll_wm_outcome_results():
    return transforms.cohort_outcome_results(
        spark.read.table(f"{BRONZE_SILVER}.silver_study_outcome_results"),
        spark.read.table(f"{BRONZE_SILVER}.gold_cll_wm_studies"))
