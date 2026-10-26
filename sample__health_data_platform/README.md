# Cascade Health sample data (synthetic)

All data is randomly generated for a demo. No real patients. Names, dates and amounts are fictional.
About 15,000 patients, 19,000 hospital stays (Jan 2024 – Sep 2026), \~135 discharges a week, \~13% readmitted within 30 days.

The demo story: a fictional health system wants to predict which discharged patients will be readmitted within 30 days,
so care managers can focus outreach on the highest-risk patients. The data comes from three source systems plus a set of
care protocol documents.

```
postgres_clinical/          Source 1: clinical records (CSV)
mysql_scheduling_billing/   Source 2: scheduling and billing (CSV)
saas_engagement/            Source 3: call center / engagement export (CSV)
protocols/                  Care protocols CP-101 to CP-105 (Markdown, unstructured)
```

#### Source 1: Postgres (clinical records) — postgres\_clinical/

* patients.csv (15,000 rows): patient\_id, first\_name, last\_name, date\_of\_birth, sex, zip\_code, insurance\_type
(about 1% of zip\_code and insurance\_type are blank on purpose, for a data quality check)
* encounters.csv (19,053 rows): encounter\_id, patient\_id, admit\_date, discharge\_date, department (Internal Medicine, Cardiology, Pulmonology, Orthopedics), primary\_diagnosis, length\_of\_stay\_days, discharge\_destination (Home, Home with home health, Skilled nursing facility)
* conditions.csv (6,746 rows): patient\_id, condition (Heart failure, COPD, Diabetes, Chronic kidney disease), diagnosed\_date

#### Source 2: MySQL (scheduling and billing) — mysql\_scheduling\_billing/

* appointments.csv (37,053 rows): appointment\_id, patient\_id, appointment\_date, appointment\_type (Lab work, Annual physical, Specialist visit, Post-discharge follow-up), provider\_department, status (Completed, No-show, Cancelled)
Follow-ups after discharge have appointment\_type = "Post-discharge follow-up".
* claims.csv (19,053 rows, one per encounter): claim\_id, encounter\_id, payer, billed\_amount, claim\_status

#### Source 3: SaaS export (patient engagement / call center) — saas\_engagement/

* outreach.csv (24,478 rows): outreach\_id, patient\_id, contact\_date, channel (Phone, Text, Portal), outcome (Reached, No answer), satisfaction\_score, notes (free text)

#### Care protocols — protocols/

Fictional care-management documents, for use as unstructured content (e.g. a document Q&A assistant or RAG demo).
Each one has a document ID, version, owner, effective date, purpose, who it applies to, and numbered steps.

| Doc | Title | Owner |
|---|---|---|
| CP-101 | Heart Failure Discharge and Transition-of-Care Protocol | Cardiology Care Management |
| CP-102 | COPD Discharge and Transition-of-Care Protocol | Pulmonary Care Management |
| CP-103 | Diabetes Follow-Up Protocol After Hospital Discharge | Endocrinology and Primary Care |
| CP-104 | High-Risk Patient Outreach Procedure (risk tiers, outreach steps, staffing) | Care Management Operations |
| CP-105 | Medication Review After Discharge | Pharmacy Services |

CP-104 defines the risk tiers (High >= 0.30, Medium 0.15–0.29, Low < 0.15) and a staffing guideline of 12–15
High-tier calls per care manager per day. CP-101, CP-102 and CP-103 say which steps apply to High and Medium patients.

#### Building the unified table (one row per discharge)

* Key: patient\_id (encounter\_id for claims).
* Label: readmitted\_30d = 1 if the same patient's next admit\_date is 0–30 days after this discharge\_date.
* Exclude discharges after 2026-08-31 (not enough time to observe a readmission).
* Useful features: age at admission, each condition flag, number of prior admissions in the last 12 months,
length\_of\_stay\_days, discharge\_destination, missed follow-up (follow-up appointment within 15 days not Completed),
reached by outreach within 4 days of discharge.

#### What a simple model should show

Logistic regression on those features gets an AUC of about 0.73. With the CP-104 tiers, roughly 8% of discharges
are High (about 42% of them readmitted), 21% Medium (about 21%) and 71% Low (about 8%).
That is about 10–12 high-risk discharges a week, which one care manager can cover under the CP-104 staffing guideline.
Main drivers: heart failure, age, prior admissions, COPD, not reached by outreach, missed follow-up appointment.
