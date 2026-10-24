"""Cascade Care Queue: care managers work their high-risk discharge list.

Screens: My work queue, Patient detail (with Log outcome and Ask the assistant), Team view.
The care manager dropdown stands in for login.
"""
import datetime as dt

import streamlit as st

import data

ACTIONS = ["Medication review scheduled", "Follow-up booked", "Home health referral", "Escalated to nurse"]
BARRIERS = ["Transportation", "Cost", "Caregiver", "Language"]
SCREENS = ["My work queue", "Patient detail", "Team view"]

st.set_page_config(page_title="Cascade Care Queue", page_icon="🩺", layout="wide", initial_sidebar_state="expanded")


@st.cache_data(ttl=600, show_spinner=False)
def summaries(encounter_ids):
    return data.patient_summaries(list(encounter_ids))


@st.cache_data(ttl=600, show_spinner=False)
def detail_bundle(encounter_id):
    detail = data.patient_detail(encounter_id)
    if detail is None:
        return None
    pid = detail["patient_id"]
    return {
        "detail": detail,
        "encounters": data.recent_encounters(pid),
        "appointments": data.followup_appointments(pid),
        "outreach": data.outreach_history(pid),
        "protocols": data.protocols(data.protocol_ids_for(detail)),
    }


def display_name(row):
    name = (row.get("patient_name") or "").strip()
    return row["patient_id"] if not name or "*" in name else name


def due_text(due_by):
    delta = due_by - data.now()
    hours = int(abs(delta.total_seconds()) // 3600)
    span = f"{hours // 24} d {hours % 24} h" if hours >= 24 else f"{hours} h"
    return f"Due in {span}" if delta.total_seconds() >= 0 else f"Overdue by {span}"


def go_to(screen, **state):
    st.session_state.update(state)
    # A new key clears the queue table selection, so Back does not reopen the same patient.
    st.session_state["table_nonce"] = st.session_state.get("table_nonce", 0) + 1
    st.session_state["next_screen"] = screen
    st.rerun()


# ---------- Sidebar ----------
if "next_screen" in st.session_state:
    st.session_state["screen"] = st.session_state.pop("next_screen")

try:
    managers = data.care_managers()
except Exception as e:  # noqa: BLE001 - show the cause instead of a blank page
    st.error("The care manager list could not be loaded, so the queue can't be shown.")
    st.exception(e)
    st.stop()
names = {m["care_manager_id"]: m["name"] for m in managers}
with st.sidebar:
    st.title("Cascade Care Queue")
    cm_id = st.selectbox("I am", list(names), format_func=names.get, key="care_manager_id")
    screen = st.radio("Go to", SCREENS, key="screen")
    st.caption(f"Today (demo date): {data.DEMO_TODAY:%B %d, %Y}")
    st.caption("Synthetic demo data. Patients are fictional.")

if flash := st.session_state.pop("flash", None):
    st.success(flash)


# ---------- Screen 1: My work queue ----------
def work_queue():
    rows = data.assignments(cm_id)
    info = summaries(tuple(sorted(r["encounter_id"] for r in rows)))
    open_count = sum(r["status"] in data.OPEN_STATUSES for r in rows)
    st.header(f"{open_count} high-risk patient{'s' if open_count != 1 else ''} to contact today")
    st.caption(f"{names[cm_id]}: {len(rows)} assigned this week, sorted by risk (highest first).")

    c1, c2 = st.columns([3, 1])
    statuses = c1.multiselect("Status", data.STATUSES, default=data.STATUSES)
    overdue_only = c2.toggle("Overdue only")

    now = data.now()
    table = []
    for r in sorted(rows, key=lambda r: -info.get(r["encounter_id"], {}).get("risk_score", 0)):
        s = info.get(r["encounter_id"], {})
        overdue = r["status"] in data.OPEN_STATUSES and r["due_by"] < now
        if r["status"] not in statuses or (overdue_only and not overdue):
            continue
        table.append({
            "encounter_id": r["encounter_id"],
            "Patient": display_name({**s, "patient_id": r["patient_id"]}),
            "Age": s.get("age_at_admission"),
            "Primary diagnosis": s.get("primary_diagnosis"),
            "Discharged": s.get("discharge_date"),
            "Risk": s.get("risk_score"),
            "Top reason": (s.get("top_3_reasons") or "").split("; ")[0],
            "Status": r["status"],
            "Due": due_text(r["due_by"]) if r["status"] in data.OPEN_STATUSES else "Done",
        })
    if not table:
        st.info("No patients match these filters.")
        return
    event = st.dataframe(
        table, hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row",
        key=f"queue_{st.session_state.get('table_nonce', 0)}",
        column_order=["Patient", "Age", "Primary diagnosis", "Discharged", "Risk", "Top reason", "Status", "Due"],
        column_config={"Risk": st.column_config.ProgressColumn("Risk", min_value=0, max_value=1, format="%.2f"),
                       "Discharged": st.column_config.DateColumn("Discharged", format="MMM D")})
    st.caption("Select a row to open the patient.")
    if event.selection.rows:
        go_to("Patient detail", encounter_id=table[event.selection.rows[0]]["encounter_id"])


# ---------- Screen 2: Patient detail ----------
def patient_detail():
    encounter_id = st.session_state.get("encounter_id")
    if not encounter_id:
        st.info("Pick a patient from **My work queue** first.")
        return
    bundle = detail_bundle(encounter_id)
    a = data.assignment(encounter_id)
    if bundle is None or a is None:
        st.error("This patient is not in the work queue.")
        return
    d = bundle["detail"]
    outcomes = data.logged_outcomes(encounter_id)

    if st.button("← Back to my work queue"):
        go_to("My work queue")
    st.header(display_name(d))
    st.caption(f"Patient {d['patient_id']} · age {d['age_at_admission']} · {d['primary_diagnosis']} · "
               f"discharged {d['discharge_date']:%b %d} to {d['discharge_destination']} · "
               f"assigned to {a['care_manager']}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Readmission risk", f"{d['risk_score']:.0%}")
    c2.metric("Risk tier", d["risk_tier"])
    c3.metric("Status", a["status"])
    c4.metric("Contact deadline", due_text(a["due_by"]) if a["status"] in data.OPEN_STATUSES else "Done")

    left, right = st.columns([3, 2], gap="large")
    with left:
        st.subheader("Why this patient is high risk")
        for reason in (d["top_3_reasons"] or "").split("; "):
            st.markdown(f"- {reason}")
        st.subheader("Recommended next steps")
        st.markdown(d["care_recommendation"] or "_No recommendation has been written for this patient yet._")
        st.caption("Written by AI from Cascade Health protocols. Review before acting.")

        st.subheader("Recent hospital stays")
        st.dataframe([{"Admitted": e["admit_date"], "Discharged": e["discharge_date"], "Department": e["department"],
                       "Diagnosis": e["primary_diagnosis"], "Days": e["length_of_stay_days"],
                       "Went to": e["discharge_destination"]} for e in bundle["encounters"]],
                     hide_index=True, width="stretch")
        st.subheader("Follow-up appointments")
        if bundle["appointments"]:
            st.dataframe([{"Date": x["appointment_date"], "Department": x["provider_department"],
                           "Status": x["status"]} for x in bundle["appointments"]],
                         hide_index=True, width="stretch")
        else:
            st.write("None scheduled.")

        st.subheader("Outreach notes")
        notes = [{"Date": o["logged_at"].date(), "By": o["care_manager"], "Result": f"Reached: {o['reached']}",
                  "Note": "; ".join(filter(None, [o["note"], ", ".join(o["actions_taken"]),
                                                  ("Barriers: " + ", ".join(o["barriers"])) if o["barriers"] else ""]))}
                 for o in outcomes]
        notes += [{"Date": o["contact_date"], "By": o["channel"], "Result": o["outcome"], "Note": o["notes"]}
                  for o in bundle["outreach"]]
        if notes:
            st.dataframe(notes, hide_index=True, width="stretch")
        else:
            st.write("No outreach recorded yet.")

    with right:
        log_outcome_form(a)
        assistant_panel(d, bundle, outcomes)


def log_outcome_form(a):
    st.subheader("Log outcome")
    with st.form("log_outcome", clear_on_submit=True, border=True):
        reached = st.radio("Did you reach the patient?", ["Yes", "No", "Wrong number"], horizontal=True)
        st.markdown("**Actions taken**")
        actions = [x for x in ACTIONS if st.checkbox(x, key=f"act_{x}")]
        st.markdown("**Barriers**")
        barriers = [x for x in BARRIERS if st.checkbox(x, key=f"bar_{x}")]
        note = st.text_area("Note", placeholder="What happened on the call?")
        if st.form_submit_button("Save outcome", type="primary"):
            status = data.log_outcome(a["encounter_id"], st.session_state["care_manager_id"], reached, actions,
                                      barriers, note)
            go_to("Patient detail", flash=f"Saved. Status is now {status}.")
    if a["status"] != "Done" and st.button("Mark as done"):
        data.set_status(a["encounter_id"], "Done")
        go_to("Patient detail", flash="Marked as done.")


def assistant_panel(d, bundle, outcomes):
    st.subheader("Ask the assistant")
    st.caption("Answers come only from this patient's record, notes and the matching protocols: "
               + ", ".join(bundle["protocols"]) + ".")
    history = st.session_state.setdefault(f"chat_{d['encounter_id']}", [])
    for q, ans in history:
        st.markdown(f"**You:** {q}")
        st.markdown(ans)
    with st.form("ask", clear_on_submit=True):
        question = st.text_input("Question", placeholder="e.g. What should I cover on the first call?")
        if st.form_submit_button("Ask") and question.strip():
            with st.spinner("Thinking..."):
                context = data.assistant_context(d, bundle["outreach"], outcomes, bundle["protocols"])
                history.append((question.strip(), data.ask_assistant(question.strip(), context)))
            st.rerun()


# ---------- Screen 3: Team view ----------
def team_view():
    per_manager, on_time, overdue = data.team_summary()
    st.header("Team view")
    c1, c2, c3 = st.columns(3)
    c1.metric("High-risk patients this week", on_time["total"])
    c2.metric("Contacted within 48 hours", f"{on_time['on_time'] / on_time['total']:.0%}" if on_time["total"] else "–")
    c3.metric("Overdue patients", len(overdue))

    st.subheader("By care manager")
    st.dataframe([{"Care manager": m["care_manager"], "Assigned": m["assigned"], "Completed": m["completed"],
                   "Overdue": m["overdue"]} for m in per_manager], hide_index=True, width="stretch")

    st.subheader("Overdue patients")
    if not overdue:
        st.write("Nobody is overdue.")
        return
    info = summaries(tuple(sorted(o["encounter_id"] for o in overdue)))
    st.dataframe([{"Patient": display_name({**info.get(o["encounter_id"], {}), "patient_id": o["patient_id"]}),
                   "Care manager": o["care_manager"], "Status": o["status"],
                   "Risk": info.get(o["encounter_id"], {}).get("risk_score"), "Due": due_text(o["due_by"])}
                  for o in overdue],
                 hide_index=True, width="stretch",
                 column_config={"Risk": st.column_config.NumberColumn("Risk", format="%.2f")})


{"My work queue": work_queue, "Patient detail": patient_detail, "Team view": team_view}[screen]()
