# dbt_tests

Two small dbt projects that model synthetic healthcare data from two
operational databases. Each runs locally against a DuckDB file that stands in for
the real database, so no server is needed. Switch the target to run the same
models against real Postgres or MySQL.

| Project | Source system | Models |
|---|---|---|
| [postgres_clinical](postgres_clinical/) | Postgres EHR: patients, encounters, conditions | `fct_encounters`: inpatient stays with demographics, chronic-condition flags and a 30-day readmission flag |
| [mysql_scheduling_billing](mysql_scheduling_billing/) | MySQL practice management: appointments, claims | `fct_appointments`: status flags and prior no-show count. `fct_claims`: billed amount split into paid, pending and denied |

The two projects share keys. `claims.encounter_id` joins to
`encounters.encounter_id`, and `patient_id` is common to both.

## Requirements

- Python 3.9+
- dbt-core 1.10+ (the tests use the `arguments:` syntax)
- `dbt-duckdb` for local runs
- Optional: `dbt-postgres` and/or `dbt-mysql` (MySQL 8+) for the real databases

```bash
pip install dbt-core dbt-duckdb
```

## Quick start (local DuckDB)

Run the following from inside either project folder:

```bash
cd postgres_clinical            # or mysql_scheduling_billing
cp profiles.example.yml profiles.yml
dbt deps                        # postgres_clinical only (dbt_utils)
dbt build --profiles-dir .      # seed, run, test
```

`dbt build` loads the CSVs in `seeds/` into the DuckDB file
(`clinical.duckdb` or `scheduling_billing.duckdb`), builds the models, and runs
every data test.

## Running against the real databases

Set the connection environment variables, then pick the target:

```bash
# Postgres
export PG_HOST=... PG_PORT=5432 PG_USER=... PG_PASSWORD=... PG_DATABASE=clinical
export PG_SOURCE_SCHEMA=public          # where the raw tables live
dbt run  --profiles-dir . --target postgres
dbt test --profiles-dir . --target postgres

# MySQL
export MYSQL_HOST=... MYSQL_PORT=3306 MYSQL_USER=... MYSQL_PASSWORD=...
export MYSQL_SOURCE_SCHEMA=scheduling_billing
dbt run  --profiles-dir . --target mysql
dbt test --profiles-dir . --target mysql
```

On the real databases the raw tables already exist, so skip `dbt seed` (and
use `run` + `test` rather than `build`, which would seed).

## Layout

```
<project>/
  seeds/             raw CSVs (synthetic data)
  models/staging/    views: rename, cast, clean; _sources.yml + _staging.yml tests
  models/marts/      tables: fact models; _marts.yml tests
  macros/            generate_schema_name and cross-database helpers
  profiles.example.yml
```

### Schemas

`generate_schema_name` is overridden so custom schema names are used exactly
as written, without the target-schema prefix.

| Project | Seeds | Staging | Marts |
|---|---|---|---|
| postgres_clinical | `public` (or `$PG_SOURCE_SCHEMA`) | `staging` | `marts` |
| mysql_scheduling_billing | `scheduling_billing` | `analytics` | `analytics` |

## Models

### postgres_clinical

- **stg_clinical__patients**: casts DOB and zip code, upper-cases `sex`.
- **stg_clinical__encounters**: casts dates and length of stay. Blank
  discharge destinations become null.
- **stg_clinical__conditions**: one row per patient per chronic condition.
- **fct_encounters**: one row per encounter, with:
  - `age_at_admit`
  - `has_diabetes`, `has_heart_failure`, `has_copd`, `has_ckd`, `chronic_condition_count`
  - `next_admit_date`, `days_to_next_admit`, `is_readmitted_30d`

### mysql_scheduling_billing

- **stg_scheduling__appointments**: normalises status to `completed` /
  `no_show` / `cancelled`.
- **stg_billing__claims**: `'Unknown'` payer becomes null; status is lower-cased.
- **fct_appointments**: one row per appointment, with:
  - status flags (`is_completed`, `is_no_show`, `is_cancelled`, `is_post_discharge`)
  - year, month and weekday
  - `patient_appointment_number`
  - `prior_no_show_count`
- **fct_claims**: one row per claim, with:
  - status flags
  - `paid_amount`, `pending_amount`, `denied_amount`
  - payer defaulted to `'Unknown'`

The `month_start` macro emits `date_format` on MySQL and `date_trunc` on DuckDB.

## Tests

Tests are declared in the YAML files next to the models:

- `unique` / `not_null` on every key
- `accepted_values` on categorical columns (status, payer, department, sex, …)
- `relationships`: encounters → patients
- `dbt_utils.unique_combination_of_columns` on (`patient_id`, `condition`)

Run them with `dbt test --profiles-dir .`, or select one model:

```bash
dbt test --profiles-dir . --select fct_encounters
```

## Data

All rows are synthetic. Row counts:

| File | Rows |
|---|---|
| patients.csv | 15,000 |
| encounters.csv | 19,053 |
| conditions.csv | 6,746 |
| appointments.csv | 37,053 |
| claims.csv | 19,053 |

## Other files

- `select_appts.agent.sql`: scratch notes for an appointments / telehealth
  SQL exercise. It is not part of either dbt project.
- `catalog.json`: empty placeholder.
