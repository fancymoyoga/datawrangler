# sample_cascade_health

Databricks bundle for the Cascade Health 30-day readmission-risk pilot (synthetic data, no real patients).

It wraps what was first built by hand in the `monicaenders` workspace so it can be versioned and redeployed.
Sample data and care protocols live in [`../cascade_data`](../cascade_data).

## What's in it

| Resource | File | What it does |
|---|---|---|
| Job: Cascade full build | `resources/cascade_build.job.yml` | On demand: bronze → silver (+ quality checks, PHI masks) → gold features → train → score → AI care recommendations → care-queue views → app grants |
| Job: Cascade daily care queue | `resources/cascade_daily_care_queue.job.yml` | 06:00 daily: score discharges, email each care manager their high-risk count |
| Dashboard: Readmission Risk | `resources/readmission_risk.dashboard.yml` | AI/BI dashboard on the gold tables |
| App: care-managers | `resources/care_managers.app.yml` | Care manager queue and assistant (Lakebase `cascade-care`, Llama 3.3 70B) |

Code: `src/sql/` (build SQL, recovered from query history), `src/notebooks/` (06 train, 07 score, 11 notify, `cascade_features.py`), `src/app/`, `src/dashboards/`.

All data lives in the `cascade_health` catalog: `cascade_bronze`, `cascade_silver`, `cascade_gold`.

## Load the sample data

The bronze SQL reads CSVs and protocols from the `cascade_health.cascade_bronze.cascade_bronze` volume:

```bash
databricks fs cp -r ../cascade_data dbfs:/Volumes/cascade_health/cascade_bronze/cascade_bronze --overwrite --profile monicaenders
```

## Deploy

```bash
databricks bundle validate --strict --profile monicaenders
databricks bundle deploy --profile monicaenders
databricks bundle run cascade_build --profile monicaenders
```

Dev mode pauses schedules and prefixes job and dashboard names with `[dev monica_enders]`, so the hand-made originals are left alone.

**Before deploying, note:**

- **The app isn't prefixed in dev mode.** Deploying would take over the live `care-managers` app. Either bind it on purpose
  (`databricks bundle deployment bind care_managers care-managers`) or rename it in `care_managers.app.yml`.
- **`send_email` defaults to `false`**, so the daily job logs notifications without sending them. Set the variable to `"true"` only for the real run.
- **The build writes to the same tables as the hand-made build.** `CREATE OR REPLACE` runs will rebuild them in place.
- **`10_app_grants.sql` grants to the current app's service principal.** Update the ID if the app is recreated.
- **Tier cutoffs disagree.** `CP-104` says High ≥ 0.40, Medium ≥ 0.20; the scoring and README in `cascade_data` use 0.30 / 0.15.
