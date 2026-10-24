# Databricks notebook source
# MAGIC %md
# MAGIC # Email each care manager their count of new high-risk patients
# MAGIC
# MAGIC Counts each care manager's `New` assignments in the Cascade Care Queue (Lakebase, read through
# MAGIC the `cascade_care` catalog) and sends one email per care manager who has any:
# MAGIC "You have N new high-risk patients to contact today. Open your work queue: <app URL>".
# MAGIC No patient names or details go into the email.
# MAGIC
# MAGIC Mail goes through the Gmail API over HTTPS (Free Edition blocks outbound SMTP), sent as the
# MAGIC `gmail_user` account with the `cascade_app` secrets `gmail_client_id`, `gmail_client_secret`
# MAGIC and `gmail_refresh_token` (set up once with `scripts/gmail_oauth_setup.py`). Every message is
# MAGIC logged to `cascade_gold.care_manager_notifications` with whether it was sent.

# COMMAND ----------

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from email.message import EmailMessage

dbutils.widgets.text("catalog", "cascade_health")
dbutils.widgets.text("queue_catalog", "cascade_care")
dbutils.widgets.text("app_url", "https://care-managers-7474645905130724.aws.databricksapps.com")
dbutils.widgets.text("send_email", "true")

CATALOG = dbutils.widgets.get("catalog")
QUEUE = dbutils.widgets.get("queue_catalog")
APP_URL = dbutils.widgets.get("app_url")
SEND = dbutils.widgets.get("send_email").strip().lower() == "true"
LOG_TABLE = f"{CATALOG}.cascade_gold.care_manager_notifications"

# COMMAND ----------

counts = spark.sql(f"""
    SELECT m.care_manager_id, m.name, m.email, count_if(a.status = 'New') AS new_patients
    FROM {QUEUE}.care.care_managers m
    LEFT JOIN {QUEUE}.care.assignments a ON a.care_manager_id = m.care_manager_id
    GROUP BY ALL ORDER BY m.name""").collect()

GMAIL_KEYS = ("gmail_client_id", "gmail_client_secret", "gmail_refresh_token")
stored = {s.key for s in dbutils.secrets.list("cascade_app")}
gmail_user = dbutils.secrets.get("cascade_app", "gmail_user")
can_send = SEND and all(k in stored for k in GMAIL_KEYS)


def gmail_access_token():
    form = {k.removeprefix("gmail_"): dbutils.secrets.get("cascade_app", k) for k in GMAIL_KEYS}
    form["grant_type"] = "refresh_token"
    request = urllib.request.Request("https://oauth2.googleapis.com/token",
                                     urllib.parse.urlencode(form).encode(),
                                     {"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(request, timeout=30) as resp:
        return json.load(resp)["access_token"]


def gmail_send(access_token, to, subject, body):
    msg = EmailMessage()
    msg["From"], msg["To"], msg["Subject"] = gmail_user, to, subject
    msg.set_content(body)
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    request = urllib.request.Request("https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
                                     json.dumps({"raw": raw}).encode(),
                                     {"Content-Type": "application/json",
                                      "Authorization": f"Bearer {access_token}"})
    with urllib.request.urlopen(request, timeout=30) as resp:
        return json.load(resp)["id"]


def message(n):
    plural = "patient" if n == 1 else "patients"
    subject = f"Cascade Care Queue: {n} new high-risk {plural} today"
    body = (f"You have {n} new high-risk {plural} to contact today. Open your work queue: {APP_URL}\n\n"
            "Cascade Health care management (synthetic demo data)")
    return subject, body


# COMMAND ----------

log = []
access_token = None
for cm in counts:
    subject, body = message(cm.new_patients)
    if cm.new_patients == 0:
        status = "skipped: no new patients"
    elif not can_send:
        status = "not sent: Gmail API not set up" if SEND else "not sent: send_email is false"
    else:
        try:
            access_token = access_token or gmail_access_token()
            status = f"sent: {gmail_send(access_token, cm.email, subject, body)}"
        except urllib.error.HTTPError as e:
            status = f"failed: HTTP {e.code}: {e.read().decode(errors='replace')}"[:500]
        except Exception as e:  # noqa: BLE001 - log the failure and keep going
            status = f"failed: {type(e).__name__}: {e}"[:500]
    log.append((str(uuid.uuid4()), datetime.now(timezone.utc), cm.care_manager_id, cm.name, cm.email,
                int(cm.new_patients), subject, body, status))
# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
    notification_id STRING COMMENT 'Unique ID of the notification',
    created_at TIMESTAMP COMMENT 'When the job prepared the message',
    care_manager_id STRING COMMENT 'Care manager the message is for',
    care_manager_name STRING COMMENT 'Care manager name',
    email STRING COMMENT 'Address the message was sent to',
    new_patients INT COMMENT 'New high-risk assignments waiting in the work queue',
    subject STRING COMMENT 'Email subject',
    body STRING COMMENT 'Email text (no patient details)',
    status STRING COMMENT 'sent (with Gmail message ID), skipped, not sent (with reason) or failed (with error)')
  COMMENT 'One row per daily care manager notification from the Cascade Care Queue job'""")
spark.createDataFrame(log, spark.table(LOG_TABLE).schema).write.mode("append").saveAsTable(LOG_TABLE)

for row in log:
    print(row[3], row[5], row[8])
dbutils.notebook.exit(str([(row[3], row[5], row[8]) for row in log]))
