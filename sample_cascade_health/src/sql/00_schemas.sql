-- Recovered from system.query.history (original hand-run build, 2026-10-21).

CREATE SCHEMA IF NOT EXISTS cascade_health.cascade_silver
COMMENT 'Cleaned and typed Cascade Health tables, one per source table';

CREATE SCHEMA IF NOT EXISTS cascade_health.cascade_gold
COMMENT 'Business-ready Cascade Health tables';
