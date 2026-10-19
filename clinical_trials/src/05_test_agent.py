# Databricks notebook source
# MAGIC %md
# MAGIC # Test the clinical trials agent
# MAGIC
# MAGIC Loads `clinical_trials_agent` from Unity Catalog and asks it questions. Set the widgets to
# MAGIC pick a model version and your own questions (separate questions with `|`).
# MAGIC
# MAGIC For each answer it shows the cited sources and latency, and flags any answer that contains an
# MAGIC email address or phone number (same patterns the gold layer scrubs). It also runs one
# MAGIC two-turn conversation to check that follow-up questions use the earlier turns.

# COMMAND ----------

# MAGIC %pip install -q "mlflow[databricks]>=3.1" "databricks-sdk>=0.50"
# MAGIC %restart_python

# COMMAND ----------

import json
import re
import time

import mlflow
from mlflow.tracking import MlflowClient

from pipeline.transforms import EMAIL_PATTERN, PHONE_PATTERN

DEFAULT_QUESTIONS = " | ".join([
    "What is the primary outcome measure of NCT03506373?",
    "How many participants were enrolled, and who sponsors the study?",
    "Are healthy volunteers accepted, and what is the minimum age?",
])

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "clinical_trials")
dbutils.widgets.text("model_version", "latest")
dbutils.widgets.text("questions", DEFAULT_QUESTIONS)

MODEL_NAME = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}.clinical_trials_agent"
QUESTIONS = [q.strip() for q in dbutils.widgets.get("questions").split("|") if q.strip()]

mlflow.set_registry_uri("databricks-uc")
version = dbutils.widgets.get("model_version").strip()
if version == "latest":
    versions = MlflowClient().search_model_versions(f"name = '{MODEL_NAME}'")
    if not versions:
        raise RuntimeError(f"No versions of {MODEL_NAME}; run clinical_trials_agent_job first")
    version = str(max(int(v.version) for v in versions))

agent = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/{version}")
print(f"loaded {MODEL_NAME} version {version}")

# COMMAND ----------


def ask(messages):
    """Send a conversation (list of {role, content}) and return the answer, sources and latency."""
    start = time.time()
    out = agent.predict({"input": messages})
    text = "".join(part["text"] for item in out["output"] for part in item.get("content", [])
                   if part.get("type") == "output_text")
    leaks = [kind for kind, pattern in (("email", EMAIL_PATTERN), ("phone", PHONE_PATTERN))
             if re.search(pattern, text)]
    return {
        "answer": text,
        "sources": out.get("custom_outputs", {}).get("sources", []),
        "seconds": round(time.time() - start, 1),
        "pii_in_answer": leaks,
    }


results = []
for question in QUESTIONS:
    r = ask([{"role": "user", "content": question}])
    results.append({"question": question, **r})
    print(f"\nQ: {question}\nsources: {r['sources']}  ({r['seconds']}s)"
          + (f"  WARNING: answer contains {r['pii_in_answer']}" if r["pii_in_answer"] else "")
          + f"\nA: {r['answer']}")

display(spark.createDataFrame(
    [(r["question"], r["answer"], ", ".join(r["sources"]), r["seconds"], ", ".join(r["pii_in_answer"]))
     for r in results],
    "question string, answer string, sources string, seconds double, pii_in_answer string",
))

# COMMAND ----------

# MAGIC %md ## Follow-up question (multi-turn)

# COMMAND ----------

first = "Which drugs does NCT03506373 combine?"
first_answer = ask([{"role": "user", "content": first}])
follow_up = "How often is each of them given?"
second_answer = ask([
    {"role": "user", "content": first},
    {"role": "assistant", "content": first_answer["answer"]},
    {"role": "user", "content": follow_up},
])
conversation = {"turns": [{"question": first, **first_answer}, {"question": follow_up, **second_answer}]}
for turn in conversation["turns"]:
    print(f"\nQ: {turn['question']}\nA: {turn['answer']}")

# COMMAND ----------

flagged = [r["question"] for r in results + conversation["turns"] if r["pii_in_answer"]]
summary = {"model": MODEL_NAME, "version": version, "answers": results, "conversation": conversation,
           "pii_flags": flagged}
if flagged:
    print(f"WARNING: personal data in answers to: {flagged}")
dbutils.notebook.exit(json.dumps(summary))
