-- Recovered from system.query.history (original hand-run build, 2026-10-21).

CREATE OR REPLACE FUNCTION cascade_health.cascade_silver.mask_phi_string(value STRING)
RETURNS STRING
COMMENT 'Column mask: shows the real value only to members of phi_access, *** to everyone else'
RETURN CASE WHEN is_account_group_member('phi_access') OR is_member('phi_access') THEN value ELSE '***' END;

CREATE OR REPLACE FUNCTION cascade_health.cascade_silver.mask_phi_date(value DATE)
RETURNS DATE
COMMENT 'Column mask: shows the real date only to members of phi_access, null to everyone else'
RETURN CASE WHEN is_account_group_member('phi_access') OR is_member('phi_access') THEN value ELSE NULL END;

ALTER TABLE cascade_health.cascade_silver.patients
ALTER COLUMN first_name SET MASK cascade_health.cascade_silver.mask_phi_string;

ALTER TABLE cascade_health.cascade_silver.patients
ALTER COLUMN last_name SET MASK cascade_health.cascade_silver.mask_phi_string;

ALTER TABLE cascade_health.cascade_silver.patients
ALTER COLUMN date_of_birth SET MASK cascade_health.cascade_silver.mask_phi_date;
