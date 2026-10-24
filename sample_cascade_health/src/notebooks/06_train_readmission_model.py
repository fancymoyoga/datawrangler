# Databricks notebook source
# MAGIC %md
# MAGIC # Train the 30-day readmission risk model
# MAGIC
# MAGIC Logistic regression on `cascade_gold.patient_discharges` (75/25 split stratified on the
# MAGIC label). Numeric inputs are standardized and text columns one-hot encoded, so coefficients
# MAGIC are comparable. Logs parameters, test AUC and a plain-English coefficient table to MLflow,
# MAGIC registers the model in Unity Catalog and points the `champion` alias at the new version.

# COMMAND ----------

import json
import math

import mlflow
from mlflow import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from cascade_features import BINARY, CATEGORICAL, FEATURES, LABEL, NUMERIC, plain_name

dbutils.widgets.text("catalog", "cascade_health")
CATALOG = dbutils.widgets.get("catalog")
TABLE = f"{CATALOG}.cascade_gold.patient_discharges"
MODEL = f"{CATALOG}.cascade_gold.readmission_risk_model"
SEED = 42

# COMMAND ----------

# scikit-learn trains in memory, so this is the one place the data leaves Spark.
data = spark.table(TABLE).select(*FEATURES, LABEL).toPandas()
X_train, X_test, y_train, y_test = train_test_split(
    data[FEATURES], data[LABEL], test_size=0.25, stratify=data[LABEL], random_state=SEED)

model = Pipeline([
    ("prep", ColumnTransformer([
        ("num", StandardScaler(), NUMERIC),
        ("bin", "passthrough", BINARY),
        # drop="first" makes each category a comparison with its column's first category.
        ("cat", OneHotEncoder(drop="first", handle_unknown="ignore"), CATEGORICAL),
    ])),
    ("clf", LogisticRegression(max_iter=1000)),
])
model.fit(X_train, y_train)
auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
print(f"test AUC: {auc:.4f}")

# COMMAND ----------

prep = model.named_steps["prep"]
names = prep.get_feature_names_out()
encoder = prep.named_transformers_["cat"]
baseline = {c: str(cats[0]) for c, cats in zip(CATEGORICAL, encoder.categories_)}


def compared_with(name):
    if name.startswith("num__"):
        return " per standard deviation"
    if name.startswith("cat__"):
        feature = next(c for c in CATEGORICAL if name[5:].startswith(c + "_"))
        return f" compared with {baseline[feature]}"
    return ""


coefs = sorted(zip(names, model.named_steps["clf"].coef_[0]), key=lambda nc: -abs(nc[1]))
top = coefs[:10]
top_features = {
    "rank": list(range(1, len(top) + 1)),
    "feature": [plain_name(n) for n, _ in top],
    "coefficient": [round(float(c), 4) for _, c in top],
    "odds_ratio": [round(math.exp(c), 2) for _, c in top],
    "effect": [
        f"{plain_name(n)} {'raises' if c > 0 else 'lowers'} the odds of readmission "
        f"{math.exp(abs(c)):.2f}x" + compared_with(n)
        for n, c in top
    ],
}
for row in zip(*top_features.values()):
    print(row)

# COMMAND ----------

user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{user}/cascade_readmission_risk")

with mlflow.start_run(run_name="logistic_regression") as run:
    mlflow.log_params({
        "model": "LogisticRegression", "max_iter": 1000, "test_size": 0.25, "stratify": LABEL,
        "random_state": SEED, "features": ",".join(FEATURES), "train_rows": len(X_train),
        "test_rows": len(X_test), "source_table": TABLE,
    })
    mlflow.log_metric("test_auc", auc)
    mlflow.log_table(top_features, "top_features.json")
    info = mlflow.sklearn.log_model(
        model, artifact_path="model", input_example=X_train.head(5), registered_model_name=MODEL)

version = info.registered_model_version
MlflowClient().set_registered_model_alias(MODEL, "champion", version)

result = {"model": MODEL, "version": version, "test_auc": round(auc, 4), "run_id": run.info.run_id,
          "top_features": top_features["effect"]}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
