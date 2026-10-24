-- Recovered from system.query.history (original hand-run build, 2026-10-21).

CREATE OR REPLACE TABLE cascade_health.cascade_silver.data_quality_results
COMMENT 'One row per data quality check on the raw Cascade Health source tables, with how many rows failed'
AS
WITH p AS (SELECT DISTINCT patient_id FROM cascade_health.cascade_bronze.patients),
e AS (SELECT DISTINCT encounter_id FROM cascade_health.cascade_bronze.encounters),
checks AS (
SELECT 'patients' AS table_name, 'insurance_type missing' AS check_name,
count(*) AS rows_checked, count_if(nullif(trim(insurance_type), '') IS NULL) AS rows_failed
FROM cascade_health.cascade_bronze.patients
UNION ALL
SELECT 'patients', 'zip_code missing', count(*), count_if(nullif(trim(zip_code), '') IS NULL)
FROM cascade_health.cascade_bronze.patients
UNION ALL
SELECT 'patients', 'duplicate patient_id', count(*), count(*) - count(DISTINCT patient_id)
FROM cascade_health.cascade_bronze.patients
UNION ALL
SELECT 'encounters', 'discharge before admit', count(*),
count_if(try_cast(discharge_date AS DATE) < try_cast(admit_date AS DATE))
FROM cascade_health.cascade_bronze.encounters
UNION ALL
SELECT 'encounters', 'length_of_stay_days <= 0', count(*),
count_if(try_cast(length_of_stay_days AS INT) <= 0)
FROM cascade_health.cascade_bronze.encounters
UNION ALL
SELECT 'encounters', 'patient_id not found in patients', count(*), count_if(p.patient_id IS NULL)
FROM cascade_health.cascade_bronze.encounters x LEFT JOIN p USING (patient_id)
UNION ALL
SELECT 'appointments', 'patient_id not found in patients', count(*), count_if(p.patient_id IS NULL)
FROM cascade_health.cascade_bronze.appointments x LEFT JOIN p USING (patient_id)
UNION ALL
SELECT 'outreach', 'patient_id not found in patients', count(*), count_if(p.patient_id IS NULL)
FROM cascade_health.cascade_bronze.outreach x LEFT JOIN p USING (patient_id)
UNION ALL
SELECT 'claims', 'encounter_id not found in encounters', count(*), count_if(e.encounter_id IS NULL)
FROM cascade_health.cascade_bronze.claims x LEFT JOIN e USING (encounter_id)
UNION ALL
SELECT 'claims', 'billed_amount <= 0', count(*),
count_if(try_cast(billed_amount AS DECIMAL(12, 2)) <= 0)
FROM cascade_health.cascade_bronze.claims
)
SELECT table_name, check_name, rows_checked, rows_failed,
round(100.0 * rows_failed / nullif(rows_checked, 0), 2) AS pct_failed,
current_timestamp() AS checked_at
FROM checks;

ALTER TABLE cascade_health.cascade_silver.data_quality_results ALTER COLUMN
table_name COMMENT 'Source table the check ran on',
check_name COMMENT 'What the check looks for',
rows_checked COMMENT 'Number of rows the check looked at',
rows_failed COMMENT 'Number of rows that failed the check',
pct_failed COMMENT 'Share of checked rows that failed, as a percentage',
checked_at COMMENT 'When the check ran';
