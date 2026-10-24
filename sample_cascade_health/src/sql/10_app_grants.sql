-- Recovered from system.query.history (original hand-run build, 2026-10-21).

GRANT USE CATALOG ON CATALOG cascade_health TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT USE SCHEMA ON SCHEMA cascade_health.cascade_gold TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT SELECT ON TABLE cascade_health.cascade_gold.readmission_risk_scores TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT SELECT ON TABLE cascade_health.cascade_gold.patient_discharges TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT SELECT ON TABLE cascade_health.cascade_gold.care_recommendations TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT SELECT ON TABLE cascade_health.cascade_gold.care_manager_workload TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT USE SCHEMA ON SCHEMA cascade_health.cascade_silver TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT SELECT ON TABLE cascade_health.cascade_silver.patients TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT SELECT ON TABLE cascade_health.cascade_silver.encounters TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT SELECT ON TABLE cascade_health.cascade_silver.appointments TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT SELECT ON TABLE cascade_health.cascade_silver.outreach TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT USE SCHEMA ON SCHEMA cascade_health.cascade_bronze TO `b52c704c-e308-451c-afbf-edc86bf4b575`;

GRANT READ VOLUME ON VOLUME cascade_health.cascade_bronze.cascade_bronze TO `b52c704c-e308-451c-afbf-edc86bf4b575`;
