"""Data access for Cascade Care Queue.

Reads analytics tables (gold, silver, protocols) through a SQL warehouse with the Databricks SQL
connector, and reads and writes the work queue (care_managers, assignments, outreach_outcomes)
in Lakebase Postgres. In the app both run as the app's service principal; locally they use the
profile in DATABRICKS_CONFIG_PROFILE.
"""
import datetime as dt
import os
import time
import uuid

import psycopg
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from psycopg.rows import dict_row

CATALOG = os.getenv("CASCADE_CATALOG", "cascade_health")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "4f8c9f03a167af21")
LAKEBASE_INSTANCE = os.getenv("LAKEBASE_INSTANCE", "cascade-care")
ASSISTANT_MODEL = os.getenv("ASSISTANT_MODEL", "databricks-meta-llama-3-3-70b-instruct")
# The data ends on 2026-08-31, so the app runs on a demo clock: today is DEMO_TODAY, at the real
# time of day (UTC). Due times and logged outcomes both use it.
DEMO_TODAY = dt.date.fromisoformat(os.getenv("DEMO_TODAY", "2026-09-01"))
PROTOCOL_DIR = f"/Volumes/{CATALOG}/cascade_bronze/cascade_bronze/protocols/"

GOLD, SILVER = f"{CATALOG}.cascade_gold", f"{CATALOG}.cascade_silver"
STATUSES = ["New", "Contacted", "Unreachable", "Done"]
OPEN_STATUSES = ("New", "Unreachable")
CONDITION_PROTOCOLS = {"has_heart_failure": "CP-101", "has_copd": "CP-102", "has_diabetes": "CP-103"}
ALWAYS_PROTOCOLS = ["CP-104", "CP-105"]

_client = None
_token = (None, 0.0)


def client():
    global _client
    if _client is None:
        _client = WorkspaceClient()
    return _client


def now():
    real = dt.datetime.now(dt.timezone.utc)
    return dt.datetime.combine(DEMO_TODAY, real.time(), tzinfo=dt.timezone.utc)


# ---------- SQL warehouse ----------

def query(statement, params=None):
    cfg = client().config
    with sql.connect(server_hostname=cfg.host, http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
                     credentials_provider=lambda: cfg.authenticate) as conn:
        with conn.cursor() as cur:
            cur.execute(statement, parameters=params or {})
            columns = [c[0] for c in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def in_list(values, prefix="v"):
    """Named markers and parameters for an IN list: (':v0, :v1', {'v0': ..., 'v1': ...})."""
    params = {f"{prefix}{i}": v for i, v in enumerate(values)}
    return ", ".join(f":{k}" for k in params), params


def patient_summaries(encounter_ids):
    """Name, age, diagnosis, risk and reasons for each encounter, keyed by encounter_id."""
    if not encounter_ids:
        return {}
    markers, params = in_list(sorted(encounter_ids))
    rows = query(f"""
        SELECT s.encounter_id, s.patient_id, s.discharge_date, s.risk_score, s.risk_tier, s.top_3_reasons,
               d.age_at_admission, d.primary_diagnosis,
               concat_ws(' ', p.first_name, p.last_name) AS patient_name
        FROM {GOLD}.readmission_risk_scores s
        JOIN {GOLD}.patient_discharges d USING (encounter_id)
        LEFT JOIN {SILVER}.patients p ON p.patient_id = s.patient_id
        WHERE s.encounter_id IN ({markers})""", params)
    return {r["encounter_id"]: r for r in rows}


def patient_detail(encounter_id):
    rows = query(f"""
        SELECT d.*, s.risk_score, s.risk_tier, s.top_3_reasons, r.care_recommendation,
               concat_ws(' ', p.first_name, p.last_name) AS patient_name
        FROM {GOLD}.patient_discharges d
        JOIN {GOLD}.readmission_risk_scores s USING (encounter_id)
        LEFT JOIN {GOLD}.care_recommendations r USING (encounter_id)
        LEFT JOIN {SILVER}.patients p ON p.patient_id = d.patient_id
        WHERE d.encounter_id = :encounter_id""", {"encounter_id": encounter_id})
    return rows[0] if rows else None


def recent_encounters(patient_id, limit=10):
    return query(f"""
        SELECT encounter_id, admit_date, discharge_date, department, primary_diagnosis,
               length_of_stay_days, discharge_destination
        FROM {SILVER}.encounters WHERE patient_id = :patient_id
        ORDER BY admit_date DESC LIMIT {int(limit)}""", {"patient_id": patient_id})


def followup_appointments(patient_id, limit=10):
    return query(f"""
        SELECT appointment_date, appointment_type, provider_department, status
        FROM {SILVER}.appointments
        WHERE patient_id = :patient_id AND appointment_type = 'Post-discharge follow-up'
        ORDER BY appointment_date DESC LIMIT {int(limit)}""", {"patient_id": patient_id})


def outreach_history(patient_id, limit=10):
    """Outreach recorded in the engagement platform (before the app existed)."""
    return query(f"""
        SELECT contact_date, channel, outcome, satisfaction_score, notes
        FROM {SILVER}.outreach WHERE patient_id = :patient_id
        ORDER BY contact_date DESC LIMIT {int(limit)}""", {"patient_id": patient_id})


def protocols(protocol_ids):
    """Full markdown text of each protocol, keyed by ID, in ID order."""
    rows = query(f"""
        SELECT regexp_extract(_metadata.file_path, '(CP-[0-9]+)', 1) AS protocol_id, value AS text
        FROM read_files('{PROTOCOL_DIR}', format => 'text', wholeText => true)""")
    return {r["protocol_id"]: r["text"] for r in sorted(rows, key=lambda r: r["protocol_id"])
            if r["protocol_id"] in set(protocol_ids)}


def protocol_ids_for(detail):
    return [p for flag, p in CONDITION_PROTOCOLS.items() if detail.get(flag) == 1] + ALWAYS_PROTOCOLS


# ---------- Lakebase ----------

def pg_connect():
    global _token
    w = client()
    token, issued = _token
    if token is None or time.time() - issued > 30 * 60:  # credentials last an hour
        token = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[LAKEBASE_INSTANCE]).token
        _token = (token, time.time())
    host = os.getenv("PGHOST") or w.database.get_database_instance(LAKEBASE_INSTANCE).read_write_dns
    user = os.getenv("PGUSER") or w.current_user.me().user_name
    return psycopg.connect(host=host, dbname=os.getenv("PGDATABASE", "databricks_postgres"), user=user,
                           password=token, port=int(os.getenv("PGPORT", "5432")), sslmode="require",
                           row_factory=dict_row)


def pg(statement, params=None):
    with pg_connect() as conn:
        return conn.execute(statement, params or {}).fetchall()


def care_managers():
    return pg("SELECT care_manager_id, name, email, hospital FROM care.care_managers ORDER BY name")


def assignments(care_manager_id=None):
    where = "WHERE a.care_manager_id = %(cm)s" if care_manager_id else ""
    return pg(f"""
        SELECT a.*, m.name AS care_manager
        FROM care.assignments a JOIN care.care_managers m USING (care_manager_id) {where}""",
              {"cm": care_manager_id})


def assignment(encounter_id):
    rows = pg("""SELECT a.*, m.name AS care_manager FROM care.assignments a
                 JOIN care.care_managers m USING (care_manager_id) WHERE encounter_id = %(e)s""",
              {"e": encounter_id})
    return rows[0] if rows else None


def logged_outcomes(encounter_id):
    return pg("""SELECT o.logged_at, m.name AS care_manager, o.reached, o.actions_taken, o.barriers, o.note
                 FROM care.outreach_outcomes o JOIN care.care_managers m USING (care_manager_id)
                 WHERE encounter_id = %(e)s ORDER BY logged_at DESC""", {"e": encounter_id})


def log_outcome(encounter_id, care_manager_id, reached, actions_taken, barriers, note):
    """Record an outreach attempt and move the assignment to Contacted or Unreachable."""
    status = "Contacted" if reached == "Yes" else "Unreachable"
    with pg_connect() as conn, conn.transaction():
        conn.execute("""INSERT INTO care.outreach_outcomes
                          (encounter_id, care_manager_id, logged_at, reached, actions_taken, barriers, note)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                     (encounter_id, care_manager_id, now(), reached, list(actions_taken), list(barriers),
                      note or None))
        conn.execute("UPDATE care.assignments SET status = %s WHERE encounter_id = %s", (status, encounter_id))
    return status


def set_status(encounter_id, status):
    with pg_connect() as conn:
        conn.execute("UPDATE care.assignments SET status = %s WHERE encounter_id = %s", (status, encounter_id))


def team_summary():
    """Per care manager: assigned, completed (Contacted or Done) and overdue counts, plus the
    share of all assignments with a successful contact logged before the 48-hour deadline."""
    t = now()
    per_manager = pg("""
        SELECT m.name AS care_manager, count(a.encounter_id) AS assigned,
               count(*) FILTER (WHERE a.status IN ('Contacted', 'Done')) AS completed,
               count(*) FILTER (WHERE a.status IN ('New', 'Unreachable') AND a.due_by < %(t)s) AS overdue
        FROM care.care_managers m LEFT JOIN care.assignments a USING (care_manager_id)
        GROUP BY m.name ORDER BY m.name""", {"t": t})
    on_time = pg("""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE EXISTS (
                 SELECT 1 FROM care.outreach_outcomes o
                 WHERE o.encounter_id = a.encounter_id AND o.reached = 'Yes' AND o.logged_at <= a.due_by
               )) AS on_time
        FROM care.assignments a""")[0]
    overdue = pg("""
        SELECT a.encounter_id, a.patient_id, m.name AS care_manager, a.status, a.due_by
        FROM care.assignments a JOIN care.care_managers m USING (care_manager_id)
        WHERE a.status IN ('New', 'Unreachable') AND a.due_by < %(t)s ORDER BY a.due_by""", {"t": t})
    return per_manager, on_time, overdue


# ---------- Assistant ----------

ASSISTANT_RULES = """You help a hospital care manager prepare for outreach to one recently discharged patient.
Answer ONLY from the context below. Do not use outside knowledge.
- Write in plain English, briefly, for a care manager (not a doctor).
- End EVERY point or sentence with its source in brackets: a protocol ID such as [CP-101], or [Patient record], [Risk model] or [Outreach notes]. A point without a source is not allowed.
- If the answer is not in the context, reply exactly: I don't have that information.
- Do not diagnose, and do not recommend medications or doses beyond what a protocol says."""

PATIENT_FIELDS = [
    ("patient_id", "Patient ID"), ("age_at_admission", "Age at admission"), ("sex", "Sex"),
    ("insurance_type", "Insurance"), ("admit_date", "Admitted"), ("discharge_date", "Discharged"),
    ("department", "Department"), ("primary_diagnosis", "Primary diagnosis"),
    ("length_of_stay_days", "Length of stay (days)"), ("discharge_destination", "Discharged to"),
    ("has_heart_failure", "Heart failure (1 = yes)"), ("has_copd", "COPD (1 = yes)"),
    ("has_diabetes", "Diabetes (1 = yes)"), ("has_ckd", "Chronic kidney disease (1 = yes)"),
    ("prior_admissions_12m", "Admissions in the prior 12 months (capped at 3)"),
    ("missed_followup", "Missed follow-up appointment (1 = yes)"),
    ("reached_by_outreach", "Reached by outreach within 4 days (1 = yes)"),
]


def assistant_context(detail, outreach, outcomes, protocol_texts):
    record = "\n".join(f"- {label}: {detail.get(key)}" for key, label in PATIENT_FIELDS)
    notes = [f"- {o['contact_date']} {o['channel']}: {o['outcome']}. {o['notes'] or ''}".strip() for o in outreach]
    notes += [f"- {o['logged_at']:%Y-%m-%d} logged in the care queue: reached {o['reached']}. "
              f"Actions: {', '.join(o['actions_taken']) or 'none'}. Barriers: {', '.join(o['barriers']) or 'none'}. "
              f"{o['note'] or ''}".strip() for o in outcomes]
    docs = "\n\n".join(f"=== {pid} ===\n{text}" for pid, text in protocol_texts.items())
    return (f"PATIENT RECORD\n{record}\n\n"
            f"RISK MODEL\n- 30-day readmission risk score: {detail['risk_score']:.2f} ({detail['risk_tier']} tier)\n"
            f"- Top reasons: {detail['top_3_reasons']}\n\n"
            f"OUTREACH NOTES\n" + ("\n".join(notes) or "- None recorded") + "\n\n"
            f"PROTOCOLS\n{docs}")


def ask_assistant(question, context):
    response = client().serving_endpoints.query(
        name=ASSISTANT_MODEL,
        messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=f"{ASSISTANT_RULES}\n\nCONTEXT\n{context}"),
                  ChatMessage(role=ChatMessageRole.USER, content=question)],
        max_tokens=600, temperature=0.1)
    return response.choices[0].message.content
