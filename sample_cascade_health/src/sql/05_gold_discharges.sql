-- Recovered from system.query.history (original hand-run build, 2026-10-21).

CREATE OR REPLACE TABLE cascade_health.cascade_gold.patient_discharges
COMMENT 'One row per hospital discharge through 2026-08-31, with patient, condition, history, follow-up and outreach features and whether the patient was readmitted within 30 days'
AS
WITH enc AS (
SELECT *,
lead(admit_date) OVER (PARTITION BY patient_id ORDER BY admit_date, encounter_id) AS next_admit_date
FROM cascade_health.cascade_silver.encounters
),
dob AS (
SELECT trim(patient_id) AS patient_id, try_cast(date_of_birth AS DATE) AS date_of_birth
FROM cascade_health.cascade_bronze.patients
),
cond AS (
SELECT patient_id,
max(CASE WHEN condition = 'Heart failure' THEN 1 ELSE 0 END) AS has_heart_failure,
max(CASE WHEN condition = 'COPD' THEN 1 ELSE 0 END) AS has_copd,
max(CASE WHEN condition = 'Diabetes' THEN 1 ELSE 0 END) AS has_diabetes,
max(CASE WHEN condition = 'Chronic kidney disease' THEN 1 ELSE 0 END) AS has_ckd
FROM cascade_health.cascade_silver.conditions
GROUP BY patient_id
),
prior AS (
SELECT e.encounter_id, least(count(p.encounter_id), 3) AS prior_admissions_12m
FROM enc e
LEFT JOIN cascade_health.cascade_silver.encounters p
ON p.patient_id = e.patient_id
AND p.admit_date >= date_sub(e.admit_date, 365)
AND p.admit_date < e.admit_date
GROUP BY e.encounter_id
),
followup AS (
SELECT e.encounter_id, max(CASE WHEN a.status = 'Completed' THEN 1 ELSE 0 END) AS followup_completed
FROM enc e
JOIN cascade_health.cascade_silver.appointments a
ON a.patient_id = e.patient_id
AND a.appointment_type = 'Post-discharge follow-up'
AND a.appointment_date BETWEEN e.discharge_date AND date_add(e.discharge_date, 15)
GROUP BY e.encounter_id
),
reached AS (
SELECT DISTINCT e.encounter_id
FROM enc e
JOIN cascade_health.cascade_silver.outreach o
ON o.patient_id = e.patient_id
AND o.outcome = 'Reached'
AND o.contact_date BETWEEN e.discharge_date AND date_add(e.discharge_date, 4)
)
SELECT
e.encounter_id,
e.patient_id,
e.admit_date,
e.discharge_date,
e.department,
e.primary_diagnosis,
e.length_of_stay_days,
e.discharge_destination,
CAST(floor(months_between(e.admit_date, d.date_of_birth) / 12) AS INT) AS age_at_admission,
p.sex,
p.insurance_type,
coalesce(c.has_heart_failure, 0) AS has_heart_failure,
coalesce(c.has_copd, 0) AS has_copd,
coalesce(c.has_diabetes, 0) AS has_diabetes,
coalesce(c.has_ckd, 0) AS has_ckd,
pr.prior_admissions_12m,
CASE WHEN coalesce(f.followup_completed, 0) = 1 THEN 0 ELSE 1 END AS missed_followup,
CASE WHEN r.encounter_id IS NOT NULL THEN 1 ELSE 0 END AS reached_by_outreach,
CASE WHEN datediff(e.next_admit_date, e.discharge_date) BETWEEN 0 AND 30 THEN 1 ELSE 0 END AS readmitted_30d
FROM enc e
LEFT JOIN dob d ON d.patient_id = e.patient_id
LEFT JOIN cascade_health.cascade_silver.patients p ON p.patient_id = e.patient_id
LEFT JOIN cond c ON c.patient_id = e.patient_id
LEFT JOIN prior pr ON pr.encounter_id = e.encounter_id
LEFT JOIN followup f ON f.encounter_id = e.encounter_id
LEFT JOIN reached r ON r.encounter_id = e.encounter_id
WHERE e.discharge_date <= DATE'2026-08-31';

ALTER TABLE cascade_health.cascade_gold.patient_discharges ALTER COLUMN
encounter_id COMMENT 'Hospital encounter (stay) that ended in this discharge',
patient_id COMMENT 'Patient who was discharged',
admit_date COMMENT 'Date the patient was admitted',
discharge_date COMMENT 'Date the patient was discharged',
department COMMENT 'Hospital department that treated the patient',
primary_diagnosis COMMENT 'Main diagnosis for the stay',
length_of_stay_days COMMENT 'Length of the stay in days',
discharge_destination COMMENT 'Where the patient went after discharge, such as Home or Skilled nursing',
age_at_admission COMMENT 'Patient age in whole years on the admit date',
sex COMMENT 'Patient sex',
insurance_type COMMENT 'Insurance coverage type, Unknown when not recorded',
has_heart_failure COMMENT '1 if the patient has a heart failure diagnosis, else 0',
has_copd COMMENT '1 if the patient has a COPD diagnosis, else 0',
has_diabetes COMMENT '1 if the patient has a diabetes diagnosis, else 0',
has_ckd COMMENT '1 if the patient has a chronic kidney disease diagnosis, else 0',
prior_admissions_12m COMMENT 'Admissions for this patient in the 365 days before this admit date, capped at 3',
missed_followup COMMENT '1 unless a post-discharge follow-up appointment 0 to 15 days after discharge was Completed',
reached_by_outreach COMMENT '1 if outreach reached the patient 0 to 4 days after discharge, else 0',
readmitted_30d COMMENT 'Label: 1 if the patient was next admitted 0 to 30 days after this discharge, else 0';
