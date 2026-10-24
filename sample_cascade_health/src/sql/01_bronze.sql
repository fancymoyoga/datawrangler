-- Recovered from system.query.history (original hand-run build, 2026-10-21).

CREATE OR REPLACE TABLE cascade_health.cascade_bronze.patients AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/cascade_health/cascade_bronze/cascade_bronze/postgres_clinical/patients.csv',
format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REPLACE TABLE cascade_health.cascade_bronze.encounters AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/cascade_health/cascade_bronze/cascade_bronze/postgres_clinical/encounters.csv',
format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REPLACE TABLE cascade_health.cascade_bronze.conditions AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/cascade_health/cascade_bronze/cascade_bronze/postgres_clinical/conditions.csv',
format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REPLACE TABLE cascade_health.cascade_bronze.appointments AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/cascade_health/cascade_bronze/cascade_bronze/mysql_scheduling_billing/appointments.csv',
format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REPLACE TABLE cascade_health.cascade_bronze.claims AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/cascade_health/cascade_bronze/cascade_bronze/mysql_scheduling_billing/claims.csv',
format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REPLACE TABLE cascade_health.cascade_bronze.outreach AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/cascade_health/cascade_bronze/cascade_bronze/saas_engagement/outreach.csv',
format => 'csv', header => true, inferColumnTypes => false);
