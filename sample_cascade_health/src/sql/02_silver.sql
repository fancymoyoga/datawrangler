-- Recovered from system.query.history (original hand-run build, 2026-10-21).

CREATE OR REPLACE TABLE cascade_health.cascade_silver.patients
COMMENT 'One row per patient from the clinical system, with typed dates and insurance gaps filled as Unknown'
AS
SELECT
trim(patient_id) AS patient_id,
trim(first_name) AS first_name,
trim(last_name) AS last_name,
try_cast(date_of_birth AS DATE) AS date_of_birth,
sex,
zip_code,
coalesce(nullif(trim(insurance_type), ''), 'Unknown') AS insurance_type,
nullif(trim(insurance_type), '') IS NULL AS insurance_missing,
regexp_extract(_source_file, '/([^/]+)/[^/]+$', 1) AS _source_system
FROM cascade_health.cascade_bronze.patients
QUALIFY row_number() OVER (PARTITION BY trim(patient_id) ORDER BY _ingested_at DESC) = 1;

ALTER TABLE cascade_health.cascade_silver.patients ALTER COLUMN
patient_id COMMENT 'Unique patient identifier, the key that links every Cascade Health table',
first_name COMMENT 'Patient first name, trimmed',
last_name COMMENT 'Patient last name, trimmed',
date_of_birth COMMENT 'Patient date of birth',
sex COMMENT 'Patient sex as recorded in the clinical system',
zip_code COMMENT 'Home ZIP code, null when not recorded',
insurance_type COMMENT 'Insurance coverage type, Unknown when the source left it blank',
insurance_missing COMMENT 'True when the source had no insurance type for this patient',
_source_system COMMENT 'Source system the row was loaded from';

CREATE OR REPLACE TABLE cascade_health.cascade_silver.encounters
COMMENT 'One row per hospital encounter, with typed dates and stays that end before they start removed'
AS
SELECT
encounter_id,
patient_id,
try_cast(admit_date AS DATE) AS admit_date,
try_cast(discharge_date AS DATE) AS discharge_date,
department,
primary_diagnosis,
try_cast(length_of_stay_days AS INT) AS length_of_stay_days,
discharge_destination,
regexp_extract(_source_file, '/([^/]+)/[^/]+$', 1) AS _source_system
FROM cascade_health.cascade_bronze.encounters
WHERE NOT coalesce(try_cast(discharge_date AS DATE) < try_cast(admit_date AS DATE), false);

ALTER TABLE cascade_health.cascade_silver.encounters ALTER COLUMN
encounter_id COMMENT 'Unique hospital encounter (stay) identifier',
patient_id COMMENT 'Patient who had the encounter',
admit_date COMMENT 'Date the patient was admitted',
discharge_date COMMENT 'Date the patient was discharged',
department COMMENT 'Hospital department that treated the patient',
primary_diagnosis COMMENT 'Main diagnosis for the stay',
length_of_stay_days COMMENT 'Length of the stay in days',
discharge_destination COMMENT 'Where the patient went after discharge, such as Home or Skilled nursing',
_source_system COMMENT 'Source system the row was loaded from';

CREATE OR REPLACE TABLE cascade_health.cascade_silver.conditions
COMMENT 'One row per patient and chronic condition, keeping the earliest diagnosis date'
AS
SELECT
patient_id,
condition,
try_cast(diagnosed_date AS DATE) AS diagnosed_date,
regexp_extract(_source_file, '/([^/]+)/[^/]+$', 1) AS _source_system
FROM cascade_health.cascade_bronze.conditions
QUALIFY row_number() OVER (PARTITION BY patient_id, condition
ORDER BY try_cast(diagnosed_date AS DATE) ASC NULLS LAST) = 1;

ALTER TABLE cascade_health.cascade_silver.conditions ALTER COLUMN
patient_id COMMENT 'Patient who has the condition',
condition COMMENT 'Chronic condition, such as Diabetes or Heart failure',
diagnosed_date COMMENT 'Date the condition was first diagnosed',
_source_system COMMENT 'Source system the row was loaded from';

CREATE OR REPLACE TABLE cascade_health.cascade_silver.appointments
COMMENT 'One row per scheduled appointment with a known status (Completed, No-show or Cancelled)'
AS
SELECT
appointment_id,
patient_id,
try_cast(appointment_date AS DATE) AS appointment_date,
appointment_type,
provider_department,
trim(status) AS status,
regexp_extract(_source_file, '/([^/]+)/[^/]+$', 1) AS _source_system
FROM cascade_health.cascade_bronze.appointments
WHERE trim(status) IN ('Completed', 'No-show', 'Cancelled');

ALTER TABLE cascade_health.cascade_silver.appointments ALTER COLUMN
appointment_id COMMENT 'Unique appointment identifier',
patient_id COMMENT 'Patient the appointment was booked for',
appointment_date COMMENT 'Date of the appointment',
appointment_type COMMENT 'Kind of visit, such as Annual physical or Lab work',
provider_department COMMENT 'Department that hosted the appointment',
status COMMENT 'Outcome of the appointment: Completed, No-show or Cancelled',
_source_system COMMENT 'Source system the row was loaded from';

CREATE OR REPLACE TABLE cascade_health.cascade_silver.claims
COMMENT 'One row per billing claim, with the billed amount as a dollar decimal'
AS
SELECT
claim_id,
encounter_id,
payer,
try_cast(billed_amount AS DECIMAL(12, 2)) AS billed_amount,
claim_status,
regexp_extract(_source_file, '/([^/]+)/[^/]+$', 1) AS _source_system
FROM cascade_health.cascade_bronze.claims;

ALTER TABLE cascade_health.cascade_silver.claims ALTER COLUMN
claim_id COMMENT 'Unique billing claim identifier',
encounter_id COMMENT 'Hospital encounter the claim bills for',
payer COMMENT 'Insurer or payer billed, such as Medicare or Commercial',
billed_amount COMMENT 'Amount billed in US dollars',
claim_status COMMENT 'Where the claim stands with the payer, such as Paid or Denied',
_source_system COMMENT 'Source system the row was loaded from';

CREATE OR REPLACE TABLE cascade_health.cascade_silver.outreach
COMMENT 'One row per patient outreach attempt from the engagement platform, with typed dates and scores'
AS
SELECT
outreach_id,
patient_id,
try_cast(contact_date AS DATE) AS contact_date,
channel,
outcome,
try_cast(try_cast(nullif(trim(satisfaction_score), '') AS DECIMAL(4, 1)) AS INT) AS satisfaction_score,
notes,
regexp_extract(_source_file, '/([^/]+)/[^/]+$', 1) AS _source_system
FROM cascade_health.cascade_bronze.outreach;

ALTER TABLE cascade_health.cascade_silver.outreach ALTER COLUMN
outreach_id COMMENT 'Unique outreach attempt identifier',
patient_id COMMENT 'Patient who was contacted',
contact_date COMMENT 'Date of the outreach attempt',
channel COMMENT 'How the patient was contacted, such as Phone or SMS',
outcome COMMENT 'Result of the attempt, such as Reached or No answer',
satisfaction_score COMMENT 'Patient satisfaction from 1 to 5, null when the patient gave no score',
notes COMMENT 'Free-text notes from the care team about the attempt',
_source_system COMMENT 'Source system the row was loaded from';
