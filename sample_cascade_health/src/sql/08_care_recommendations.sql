-- Recovered from system.query.history (original hand-run build, 2026-10-21).

CREATE OR REPLACE TABLE cascade_health.cascade_gold.care_recommendations
COMMENT 'AI-written care recommendations for the latest week of High-tier discharges: why the patient is high risk and three next steps citing Cascade Health protocols'
AS
WITH protocols AS (
SELECT regexp_extract(_metadata.file_path, '(CP-[0-9]+)', 1) AS protocol_id, value AS protocol_text
FROM read_files('/Volumes/cascade_health/cascade_bronze/cascade_bronze/protocols/',
format => 'text', wholeText => true)
),
latest AS (
SELECT max(date_trunc('WEEK', discharge_date)) AS week_start
FROM cascade_health.cascade_gold.readmission_risk_scores
WHERE risk_tier = 'High'
),
patients AS (
SELECT s.patient_id, s.encounter_id, s.discharge_date, s.risk_score, s.top_3_reasons,
d.age_at_admission, d.prior_admissions_12m, d.missed_followup, d.reached_by_outreach,
d.has_heart_failure, d.has_copd, d.has_diabetes,
concat_ws(', ',
CASE WHEN d.has_heart_failure = 1 THEN 'Heart failure' END,
CASE WHEN d.has_copd = 1 THEN 'COPD' END,
CASE WHEN d.has_diabetes = 1 THEN 'Diabetes' END,
CASE WHEN d.has_ckd = 1 THEN 'Chronic kidney disease' END) AS conditions
FROM cascade_health.cascade_gold.readmission_risk_scores s
JOIN cascade_health.cascade_gold.patient_discharges d USING (encounter_id)
CROSS JOIN latest
WHERE s.risk_tier = 'High' AND date_trunc('WEEK', s.discharge_date) = latest.week_start
),
matched AS (
SELECT p.encounter_id,
array_join(array_sort(collect_list(pr.protocol_id)), ', ') AS protocols_used,
array_join(transform(array_sort(collect_list(named_struct('id', pr.protocol_id, 'text', pr.protocol_text))),
x -> x.text), '\n\n---\n\n') AS protocol_texts
FROM patients p
JOIN protocols pr
ON pr.protocol_id IN ('CP-104', 'CP-105')
OR (pr.protocol_id = 'CP-101' AND p.has_heart_failure = 1)
OR (pr.protocol_id = 'CP-102' AND p.has_copd = 1)
OR (pr.protocol_id = 'CP-103' AND p.has_diabetes = 1)
GROUP BY p.encounter_id
),
prompts AS (
SELECT p.*, m.protocols_used,
concat(
'You are helping a hospital care manager plan outreach after a discharge. ',
'Write in plain English for the care manager. Do not diagnose or suggest a diagnosis, ',
'and do not recommend specific medications or doses.\n\n',
'Patient facts:\n',
'- Age: ', p.age_at_admission, '\n',
'- Chronic conditions on record: ', coalesce(nullif(p.conditions, ''), 'None recorded'), '\n',
'- Hospital admissions in the 12 months before this stay: ', p.prior_admissions_12m,
CASE WHEN p.prior_admissions_12m = 3 THEN ' or more' ELSE '' END, '\n',
'- Missed post-discharge follow-up appointment: ', CASE WHEN p.missed_followup = 1 THEN 'Yes' ELSE 'No' END, '\n',
'- Reached by outreach within 4 days of discharge: ', CASE WHEN p.reached_by_outreach = 1 THEN 'Yes' ELSE 'No' END, '\n',
'- 30-day readmission risk score: ', format_number(p.risk_score, 2), ' (High tier)\n',
'- Top reasons from the risk model: ', p.top_3_reasons, '\n\n',
'Protocols (', m.protocols_used, '):\n\n', m.protocol_texts, '\n\n',
'Respond with exactly this format and nothing else:\n',
'Two sentences explaining why this patient is high risk, based only on the facts above.\n',
'1. <next step> (<protocol ID>)\n',
'2. <next step> (<protocol ID>)\n',
'3. <next step> (<protocol ID>)\n',
'Each step must cite one of the protocol IDs listed above.'
) AS prompt
FROM patients p
JOIN matched m USING (encounter_id)
)
SELECT patient_id, encounter_id, discharge_date, risk_score, top_3_reasons, protocols_used,
ai_query('databricks-meta-llama-3-3-70b-instruct', prompt,
modelParameters => named_struct('max_tokens', 400, 'temperature', 0.2)) AS care_recommendation,
'databricks-meta-llama-3-3-70b-instruct' AS model,
current_timestamp() AS generated_at
FROM prompts;

ALTER TABLE cascade_health.cascade_gold.care_recommendations ALTER COLUMN
patient_id COMMENT 'Patient who was discharged',
encounter_id COMMENT 'Hospital encounter that ended in this discharge',
discharge_date COMMENT 'Date the patient was discharged',
risk_score COMMENT 'Predicted probability (0 to 1) of readmission within 30 days',
top_3_reasons COMMENT 'Factors that raise this discharge risk the most, from the risk model',
protocols_used COMMENT 'Protocol documents given to the model for this patient',
care_recommendation COMMENT 'AI-written: two sentences on why the patient is high risk, then three next steps citing protocol IDs. Review before acting.',
model COMMENT 'Foundation model endpoint that wrote the recommendation',
generated_at COMMENT 'When the recommendation was written';
