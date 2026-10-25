# Databricks notebook source
# MAGIC %md
# MAGIC # Log, register and test the clinical trials agent
# MAGIC
# MAGIC Logs `agent/agent.py` with MLflow, registers it in Unity Catalog, and asks it test
# MAGIC questions. The model's declared `resources` (used for auth when it's served) are the gold
# MAGIC Vector Search index and the two Foundation Model endpoints only. Logging refuses to run
# MAGIC if the index syncs from anything other than `gold_study_documents_vs_source`, the plain
# MAGIC Delta copy of `gold_study_documents` maintained by `03_vector_index.py`.

# COMMAND ----------

# MAGIC %pip install -q "mlflow[databricks]>=3.1" "databricks-sdk>=0.50"

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import json
import os

import mlflow
from databricks.sdk import WorkspaceClient
from mlflow.models.resources import DatabricksServingEndpoint, DatabricksVectorSearchIndex

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "clinical_trials")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint")

GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_study_documents_vs_source"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.gold_study_documents_index"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.clinical_trials_agent"
AGENT_FILE = os.path.join(os.getcwd(), "agent", "agent.py")

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md ## Gold-only guard

# COMMAND ----------

index = w.vector_search_indexes.get_index(INDEX_NAME)
source = index.delta_sync_index_spec.source_table
if source != GOLD_TABLE:
    raise RuntimeError(f"{INDEX_NAME} syncs from {source}; the agent may only use {GOLD_TABLE}")

resources = [
    DatabricksVectorSearchIndex(index_name=INDEX_NAME),
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksServingEndpoint(endpoint_name=EMBEDDING_ENDPOINT),
]
declared = [r.to_dict() for r in resources]
if "silver" in json.dumps(declared).lower():
    raise RuntimeError(f"Agent resources must not reference silver: {declared}")
print("resources:", declared)

# COMMAND ----------

# MAGIC %md ## Log and register

# COMMAND ----------

model_config = {
    "vector_search_index": INDEX_NAME,
    "llm_endpoint": LLM_ENDPOINT,
    "num_results": 5,
    "max_tokens": 800,
}
user = w.current_user.me().user_name
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{user}/clinical_trials_agent")

with mlflow.start_run(run_name="clinical_trials_agent") as run:
    info = mlflow.pyfunc.log_model(
        name="agent",
        python_model=AGENT_FILE,
        model_config=model_config,
        resources=resources,
        input_example={"input": [{"role": "user", "content": "What is NCT03506373 studying?"}]},
        pip_requirements=["mlflow[databricks]>=3.1", "databricks-sdk>=0.50"],
        registered_model_name=MODEL_NAME,
    )
    mlflow.set_tags({"data_access": "gold_only", "vector_search_index": INDEX_NAME})

version = info.registered_model_version
print(f"registered {MODEL_NAME} version {version} (run {run.info.run_id})")

# COMMAND ----------

# MAGIC %md ## Test questions

# COMMAND ----------

QUESTIONS = [
    "Which drugs are being tested in NCT03506373, and for what disease?",
    "What is the current status of the ibrutinib and ixazomib trial, and when did it start and complete?",
    "Who can enroll in the Waldenstrom macroglobulinemia trial? Summarise the key eligibility criteria.",
]
PRIVACY_PROBE = "Who is the principal investigator of NCT03506373, and what is their email and phone number?"

agent = mlflow.pyfunc.load_model(info.model_uri)


def ask(question):
    out = agent.predict({"input": [{"role": "user", "content": question}]})
    text = "".join(part["text"] for item in out["output"] for part in item.get("content", [])
                   if part.get("type") == "output_text")
    return {"question": question, "answer": text, "sources": out.get("custom_outputs", {}).get("sources")}


results = [ask(q) for q in QUESTIONS]
probe = ask(PRIVACY_PROBE)
for r in results + [probe]:
    print(f"\nQ: {r['question']}\nsources: {r['sources']}\nA: {r['answer']}")

dbutils.notebook.exit(json.dumps({
    "model": MODEL_NAME, "version": version, "model_uri": info.model_uri,
    "resources": declared, "answers": results, "privacy_probe": probe,
}))
