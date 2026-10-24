# Cascade Health sample data (synthetic)

All data is randomly generated for a demo. No real patients. Names, dates and amounts are fictional.
About 15,000 patients, 19,000 hospital stays (Jan 2024 – Sep 2026), \~137 discharges a week, \~13% readmitted within 30 days.



#### Source 1: Postgres (clinical records) — postgres\_clinical/

* patients.csv: patient\_id, first\_name, last\_name, date\_of\_birth, sex, zip\_code, insurance\_type
(about 1% of zip\_code and insurance\_type are blank on purpose, for a data quality check)
* encounters.csv: encounter\_id, patient\_id, admit\_date, discharge\_date, department, primary\_diagnosis, length\_of\_stay\_days, discharge\_destination
* conditions.csv: patient\_id, condition (Heart failure, COPD, Diabetes, Chronic kidney disease), diagnosed\_date

#### Source 2: MySQL (scheduling and billing) — mysql\_scheduling\_billing/

* appointments.csv: appointment\_id, patient\_id, appointment\_date, appointment\_type, provider\_department, status (Completed, No-show, Cancelled)
Follow-ups after discharge have appointment\_type = "Post-discharge follow-up".
* claims.csv: claim\_id, encounter\_id, payer, billed\_amount, claim\_status

#### Source 3: SaaS export (patient engagement / call center) — saas\_engagement/

* outreach.csv: outreach\_id, patient\_id, contact\_date, channel, outcome (Reached, No answer), satisfaction\_score, notes (free text)

#### Building the unified table (one row per discharge)

* Key: patient\_id (encounter\_id for claims).
* Label: readmitted\_30d = 1 if the same patient's next admit\_date is 0–30 days after this discharge\_date.
* Exclude discharges after 2026-08-31 (not enough time to observe a readmission).
* Useful features: age at admission, each condition flag, number of prior admissions in the last 12 months,
length\_of\_stay\_days, discharge\_destination, missed follow-up (follow-up appointment within 15 days not Completed),
reached by outreach within 4 days of discharge.

#### What a simple model should show

Logistic regression on those features gets an AUC of about 0.73. With the CP-104 tiers
(High >= 0.30, Medium 0.15–0.29, Low < 0.15), roughly 9% of discharges are High (about 44% of them readmitted),
21% Medium (about 20%) and 70% Low (about 8%). That is about 12 high-risk discharges a week.
Strongest drivers: missed follow-up appointment, not reached by outreach, prior admissions, heart failure, COPD, age.

