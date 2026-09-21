CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS monitoring.commute_validation (
    employee_id integer PRIMARY KEY REFERENCES raw.employees(employee_id) ON DELETE CASCADE,
    home_address text NOT NULL,
    company_address text NOT NULL,
    commute_mode text NOT NULL,
    google_travel_mode text,
    distance_m integer,
    distance_km numeric(10,2),
    threshold_km numeric(10,2),
    validation_status text NOT NULL CHECK (validation_status IN ('VALID','ANOMALY','NOT_APPLICABLE','ERROR')),
    anomaly_reason text,
    api_error text,
    checked_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW analytics.v_commute_anomalies AS
SELECT cv.employee_id,e.first_name,e.last_name,cv.home_address,cv.commute_mode,
       cv.distance_km,cv.threshold_km,cv.anomaly_reason,cv.checked_at
FROM monitoring.commute_validation cv
JOIN raw.employees e USING(employee_id)
WHERE cv.validation_status='ANOMALY';
