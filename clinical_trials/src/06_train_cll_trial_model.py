# Databricks notebook source
# MAGIC %md
# MAGIC # Train the CLL trial completion model
# MAGIC
# MAGIC Predicts whether a CLL trial ends early (terminated or withdrawn) or completes, from the
# MAGIC design-time features in `gold_cll_trial_features`. Compares logistic regression with
# MAGIC gradient boosting by cross-validation, logs the better one to MLflow, registers it in
# MAGIC Unity Catalog (alias `champion`), and writes:
# MAGIC
# MAGIC - `gold_cll_trial_risk_scores`: the probability that each ongoing CLL trial ends early
# MAGIC - `gold_cll_drug_class_summary`: per treatment class, how many trials completed or ended early
# MAGIC
# MAGIC This is a model of **trial operations**, not of patient outcomes: registry data has no
# MAGIC patient-level results, so it can't say which treatment works best for a patient.

# COMMAND ----------

# MAGIC %pip install -q "mlflow[databricks]>=3.1" "scikit-learn>=1.4"

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import json

import mlflow
import numpy as np
import pandas as pd
from databricks.sdk import WorkspaceClient
from mlflow.models import infer_signature
from pyspark.sql import functions as F
from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, average_precision_score, brier_score_loss, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

dbutils.widgets.text("catalog", "clinicaltrials")
dbutils.widgets.text("schema", "clinical_trials")
# Trials that started later are left out of training: of those, mostly the ones that ended early
# have finished yet, which would teach the model that recent trials fail.
dbutils.widgets.text("max_train_start_year", "2019")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MAX_TRAIN_START_YEAR = int(dbutils.widgets.get("max_train_start_year"))
FEATURES_TABLE = f"{CATALOG}.{SCHEMA}.gold_cll_trial_features"
SCORES_TABLE = f"{CATALOG}.{SCHEMA}.gold_cll_trial_risk_scores"
SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.gold_cll_drug_class_summary"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.cll_trial_completion_model"
LABEL = "ended_early"
ID_COLUMNS = ["nct_id", "brief_title", "overall_status", LABEL, "is_ongoing"]
# start_year only encodes the censoring above; ongoing trials are all later than the training set.
EXCLUDED_FEATURES = ["start_year"]
MIN_LABELED = 200
SEED = 42

# COMMAND ----------

# MAGIC %md ## Data

# COMMAND ----------

features = spark.read.table(FEATURES_TABLE)
pdf = features.toPandas()
feature_columns = [c for c in pdf.columns if c not in ID_COLUMNS + EXCLUDED_FEATURES]
bool_columns = [c for c, t in features.dtypes if t == "boolean" and c in feature_columns]
categorical = [c for c, t in features.dtypes if t == "string" and c in feature_columns]
numeric = [c for c in feature_columns if c not in categorical]
drug_classes = [c for c in bool_columns if not c.startswith("has_") and c != "also_wm"]


def model_input(df):
    """Booleans as 0 / 1 and numbers as floats, so MLflow's signature has no nullable ints."""
    x = df[feature_columns].copy()
    x[numeric] = x[numeric].astype("float64")
    return x


labeled = pdf[pdf[LABEL].notna() & (pdf["start_year"] <= MAX_TRAIN_START_YEAR)]
if len(labeled) < MIN_LABELED:
    raise RuntimeError(f"only {len(labeled)} labeled CLL trials in {FEATURES_TABLE}; need {MIN_LABELED}")
X, y = model_input(labeled), labeled[LABEL].astype(int)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=SEED)
print(f"{len(labeled)} labeled trials started by {MAX_TRAIN_START_YEAR} ({y.mean():.1%} ended early); "
      f"train {len(X_train)}, test {len(X_test)}")
print(f"{len(categorical)} categorical, {len(numeric)} numeric features")

# COMMAND ----------

# MAGIC %md ## Compare models (5-fold CV on the training set)

# COMMAND ----------


def preprocess(scale):
    num = [SimpleImputer(strategy="median", add_indicator=True)] + ([StandardScaler()] if scale else [])
    return ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="infrequent_if_exist", min_frequency=10), categorical),
        ("num", make_pipeline(*num), numeric),
    ])


# No class weighting, so p_ended_early stays a calibrated probability.
candidates = {
    "logistic_regression": make_pipeline(
        preprocess(scale=True),
        LogisticRegression(C=0.3, max_iter=2000)),
    "gradient_boosting": make_pipeline(
        preprocess(scale=False),
        HistGradientBoostingClassifier(learning_rate=0.05, max_iter=300, max_depth=3, min_samples_leaf=20,
                                       l2_regularization=1.0, random_state=SEED)),
}
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=SEED)
cv_auc = {name: cross_val_score(m, X_train, y_train, cv=cv, scoring="roc_auc") for name, m in candidates.items()}
for name, scores in cv_auc.items():
    print(f"{name}: CV ROC AUC {scores.mean():.3f} ± {scores.std():.3f}")
best_name = max(cv_auc, key=lambda n: cv_auc[n].mean())
print("best:", best_name)

# COMMAND ----------

# MAGIC %md ## Fit, evaluate on the held-out test set, log and register

# COMMAND ----------

model = clone(candidates[best_name]).fit(X_train, y_train)
p_test = model.predict_proba(X_test)[:, 1]
metrics = {
    "cv_roc_auc_mean": float(cv_auc[best_name].mean()),
    "cv_roc_auc_std": float(cv_auc[best_name].std()),
    **{f"cv_roc_auc_{n}": float(s.mean()) for n, s in cv_auc.items()},
    "test_roc_auc": roc_auc_score(y_test, p_test),
    "test_average_precision": average_precision_score(y_test, p_test),
    "test_brier": brier_score_loss(y_test, p_test),
    "test_accuracy": accuracy_score(y_test, p_test >= 0.5),
    "test_base_rate": float(y_test.mean()),
    "n_labeled": len(labeled),
}
print(json.dumps({k: round(v, 3) for k, v in metrics.items()}, indent=2))

perm = permutation_importance(model, X_test, y_test, scoring="roc_auc", n_repeats=20, random_state=SEED)
importance = (pd.DataFrame({"feature": feature_columns, "importance": perm.importances_mean,
                            "importance_std": perm.importances_std})
              .sort_values("importance", ascending=False).reset_index(drop=True))
print(importance.head(15).to_string(index=False))

user = WorkspaceClient().current_user.me().user_name
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{user}/cll_trial_completion_model")

with mlflow.start_run(run_name=best_name) as run:
    mlflow.log_params({"model": best_name, "features_table": FEATURES_TABLE, "label": LABEL,
                       "test_size": 0.2, "seed": SEED, "max_train_start_year": MAX_TRAIN_START_YEAR,
                       "excluded_features": ",".join(EXCLUDED_FEATURES)})
    mlflow.log_params({f"{best_name}__{k}": v for k, v in candidates[best_name].steps[-1][1].get_params().items()
                       if isinstance(v, (int, float, str, bool)) or v is None})
    mlflow.log_metrics(metrics)
    mlflow.log_table(importance, "permutation_importance.json")
    info = mlflow.sklearn.log_model(
        model, name="model",
        signature=infer_signature(X_train, model.predict_proba(X_train)[:, 1]),
        input_example=X_train.head(3),
        registered_model_name=MODEL_NAME,
    )
    mlflow.set_tags({"task": "binary classification: ended_early", "cohort": "CLL"})

version = info.registered_model_version
mlflow.MlflowClient().set_registered_model_alias(MODEL_NAME, "champion", version)
print(f"registered {MODEL_NAME} version {version} as champion (run {run.info.run_id})")

# COMMAND ----------

# MAGIC %md ## Score ongoing trials and summarise by treatment class

# COMMAND ----------

ongoing = pdf[pdf["is_ongoing"]].copy()
ongoing["p_ended_early"] = model.predict_proba(model_input(ongoing))[:, 1] if len(ongoing) else []
scored_at = pd.Timestamp.now(tz="UTC")
scores = ongoing[["nct_id", "brief_title", "overall_status", "start_year", "phase", "p_ended_early"]].assign(
    risk_band=lambda d: pd.cut(d["p_ended_early"], [0, 0.33, 0.66, 1], labels=["low", "medium", "high"],
                               include_lowest=True).astype(str),
    drug_classes=ongoing[drug_classes].apply(lambda r: [c for c in drug_classes if r[c]], axis=1),
    model_name=MODEL_NAME, model_version=str(version), scored_at=scored_at,
).sort_values("p_ended_early", ascending=False)
(spark.createDataFrame(scores).write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(SCORES_TABLE))
spark.sql(f"COMMENT ON TABLE {SCORES_TABLE} IS 'Probability that each ongoing CLL trial ends early "
          f"(terminated / withdrawn), from {MODEL_NAME} version {version}'")

pdf["p_ended_early"] = model.predict_proba(model_input(pdf))[:, 1]
summary = pd.DataFrame([{
    "drug_class": c,
    "n_trials": int(pdf[c].sum()),
    "n_completed": int((pdf[c] & (pdf[LABEL] == 0)).sum()),
    "n_ended_early": int((pdf[c] & (pdf[LABEL] == 1)).sum()),
    "n_ongoing": int((pdf[c] & pdf["is_ongoing"]).sum()),
    "mean_p_ended_early_ongoing": float(pdf.loc[pdf[c] & pdf["is_ongoing"], "p_ended_early"].mean())
    if (pdf[c] & pdf["is_ongoing"]).any() else None,
    "permutation_importance": float(importance.set_index("feature").loc[c, "importance"]),
} for c in drug_classes])
finished = summary["n_completed"] + summary["n_ended_early"]
summary["ended_early_rate"] = summary["n_ended_early"] / finished.replace(0, np.nan)
summary = summary.sort_values("n_trials", ascending=False)
(spark.createDataFrame(summary.astype(object).where(summary.notna(), None)).write.mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(SUMMARY_TABLE))
spark.sql(f"COMMENT ON TABLE {SUMMARY_TABLE} IS 'CLL trials per treatment class: completed, ended early, "
          f"ongoing, mean predicted risk of the ongoing ones (descriptive: not treatment efficacy)'")
print(summary.to_string(index=False))

dbutils.notebook.exit(json.dumps({
    "model": MODEL_NAME, "version": version, "best": best_name,
    "metrics": {k: round(v, 4) for k, v in metrics.items()},
    "top_features": importance.head(10).round(4).to_dict("records"),
    "scored_ongoing": len(scores), "scores_table": SCORES_TABLE, "summary_table": SUMMARY_TABLE,
}))
