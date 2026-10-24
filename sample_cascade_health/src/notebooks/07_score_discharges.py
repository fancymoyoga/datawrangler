# Databricks notebook source
# MAGIC %md
# MAGIC # Score every discharge for 30-day readmission risk
# MAGIC
# MAGIC Loads the `champion` version of `cascade_gold.readmission_risk_model` and applies its
# MAGIC logistic regression formula in Spark (same result as `predict_proba`, checked on a sample),
# MAGIC then writes `cascade_gold.readmission_risk_scores`.
# MAGIC
# MAGIC **top_3_reasons:** each input's contribution is its coefficient times how far this discharge
# MAGIC is from the average discharge. The three largest contributions that raise the risk are named.

# COMMAND ----------

import json
from functools import reduce

import mlflow
from pyspark.sql import functions as F

from cascade_features import FEATURES, LABELS, split_name, plain_name

dbutils.widgets.text("catalog", "cascade_health")
CATALOG = dbutils.widgets.get("catalog")
SOURCE = f"{CATALOG}.cascade_gold.patient_discharges"
TARGET = f"{CATALOG}.cascade_gold.readmission_risk_scores"
MODEL = f"{CATALOG}.cascade_gold.readmission_risk_model"

mlflow.set_registry_uri("databricks-uc")
model = mlflow.sklearn.load_model(f"models:/{MODEL}@champion")
prep, clf = model.named_steps["prep"], model.named_steps["clf"]
scaler = prep.named_transformers_["num"]
scale = dict(zip(scaler.feature_names_in_, zip(scaler.mean_, scaler.scale_)))

# COMMAND ----------

# The value each model input takes after preprocessing, as a Spark column.
inputs = []
for name, coef in zip(prep.get_feature_names_out(), clf.coef_[0]):
    kind, feature, category = split_name(name)
    if kind == "num":
        mean, sd = scale[feature]
        value = (F.col(feature) - F.lit(mean)) / F.lit(sd)
    elif kind == "bin":
        value = F.col(feature).cast("double")
    else:
        value = F.when(F.col(feature) == F.lit(category), 1.0).otherwise(0.0)
    inputs.append((name, kind, feature, float(coef), value))

discharges = spark.table(SOURCE)
x = discharges.select("patient_id", "encounter_id", "discharge_date", *[v.alias(n) for n, _, _, _, v in inputs])
means = x.agg(*[F.avg(n).alias(n) for n, *_ in inputs]).first().asDict()

logit = reduce(lambda a, b: a + b, [F.lit(c) * F.col(n) for n, _, _, c, _ in inputs], F.lit(float(clf.intercept_[0])))

reasons = []
for name, kind, feature, coef, _ in inputs:
    delta = F.col(name) - F.lit(means[name])
    if kind == "cat":
        # Only the patient's own category is named, e.g. "Discharged to Skilled nursing".
        contribution = F.when(F.col(name) == 1, F.lit(coef) * delta)
        label = F.lit(plain_name(name))
    else:
        contribution = F.lit(coef) * delta
        high, low = LABELS[feature]
        label = F.when(delta > 0, F.lit(high)).otherwise(F.lit(low))
    # sort_array sorts structs by their first field, so the negated contribution puts the largest first.
    reasons.append(F.struct((-contribution).alias("neg"), label.alias("label")))

ranked = F.sort_array(F.filter(F.array(*reasons), lambda r: r["neg"] < 0))
scores = x.select(
    "patient_id", "encounter_id", "discharge_date",
    (1 / (1 + F.exp(-logit))).alias("risk_score"),
    F.array_join(F.transform(F.slice(ranked, 1, 3), lambda r: r["label"]), "; ").alias("top_3_reasons"),
).withColumn(
    "risk_tier",
    F.when(F.col("risk_score") >= 0.30, "High").when(F.col("risk_score") >= 0.15, "Medium").otherwise("Low"),
).withColumn("scored_at", F.current_timestamp()).select(
    "patient_id", "encounter_id", "discharge_date", "risk_score", "risk_tier", "top_3_reasons", "scored_at")

# COMMAND ----------

# Check the Spark formula against the model itself on a sample.
sample = discharges.orderBy("encounter_id").limit(500)
expected = model.predict_proba(sample.select(*FEATURES).toPandas())[:, 1]
actual = [r.risk_score for r in scores.join(sample.select("encounter_id"), "encounter_id")
          .orderBy("encounter_id").select("risk_score").collect()]
max_diff = max(abs(a - e) for a, e in zip(actual, expected))
assert len(actual) == len(expected) and max_diff < 1e-9, max_diff

# COMMAND ----------

scores.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)
spark.sql(f"""COMMENT ON TABLE {TARGET} IS 'Predicted 30-day readmission risk for every discharge in patient_discharges, with a risk tier and the top three reasons in plain English'""")
for column, comment in {
    "patient_id": "Patient who was discharged",
    "encounter_id": "Hospital encounter that ended in this discharge",
    "discharge_date": "Date the patient was discharged",
    "risk_score": "Predicted probability (0 to 1) that the patient is readmitted within 30 days",
    "risk_tier": "High if risk_score is 0.30 or more, Medium if 0.15 to under 0.30, Low if under 0.15",
    "top_3_reasons": "Up to three factors that raise the risk most for this discharge, largest first",
    "scored_at": "When the model scored this discharge",
}.items():
    spark.sql(f"ALTER TABLE {TARGET} ALTER COLUMN {column} COMMENT '{comment}'")

version = mlflow.MlflowClient().get_model_version_by_alias(MODEL, "champion").version
result = {"rows": spark.table(TARGET).count(), "model_version": version, "max_diff_vs_sklearn": max_diff}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
