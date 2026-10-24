-- Recovered from system.query.history (original hand-run build, 2026-10-21).

CREATE OR REPLACE VIEW cascade_health.cascade_gold.care_manager_workload (
week_start COMMENT 'Monday that starts the discharge week',
high_tier_discharges COMMENT 'High-tier discharges in the week, each needing an outreach call',
care_managers_needed COMMENT 'Care managers needed at 60 High-tier calls per manager per week, rounded up'
)
COMMENT 'Weekly High-tier discharge volume and the care managers needed to call them, at 60 calls per manager per week (12 a day, protocol CP-104)'
AS
SELECT date_trunc('WEEK', discharge_date)::DATE AS week_start,
count(*) AS high_tier_discharges,
CAST(ceil(count(*) / 60.0) AS INT) AS care_managers_needed
FROM cascade_health.cascade_gold.readmission_risk_scores
WHERE risk_tier = 'High'
GROUP BY 1;

CREATE OR REPLACE VIEW cascade_health.cascade_gold.care_managers
COMMENT 'Care managers who work the high-risk queue (fictional). Live view of the Lakebase table care.care_managers'
AS SELECT care_manager_id, name, email, hospital FROM cascade_care.care.care_managers;

CREATE OR REPLACE VIEW cascade_health.cascade_gold.care_queue_assignments
COMMENT 'One row per High-tier discharge assigned to a care manager, due 48 hours after discharge. Live view of the Lakebase table care.assignments'
AS SELECT encounter_id, patient_id, care_manager_id, assigned_at, status, due_by FROM cascade_care.care.assignments;

CREATE OR REPLACE VIEW cascade_health.cascade_gold.outreach_outcomes
COMMENT 'One row per outreach attempt logged in the Cascade Care Queue app. Live view of the Lakebase table care.outreach_outcomes'
AS SELECT outcome_id, encounter_id, care_manager_id, logged_at, reached, actions_taken, barriers, note
FROM cascade_care.care.outreach_outcomes;
