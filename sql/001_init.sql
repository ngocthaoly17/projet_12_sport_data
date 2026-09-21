CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS raw.employees (
    employee_id integer PRIMARY KEY,
    last_name text,
    first_name text,
    birth_date date,
    business_unit text,
    hire_date date,
    annual_gross_salary numeric(12,2),
    contract_type text,
    paid_leave_days integer,
    home_address text,
    commute_mode text,
    ingested_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.employee_sports (
    employee_id integer PRIMARY KEY REFERENCES raw.employees(employee_id),
    sport text,
    ingested_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.activities (
    activity_id bigserial PRIMARY KEY,
    employee_id integer NOT NULL REFERENCES raw.employees(employee_id),
    start_at timestamptz NOT NULL,
    end_at timestamptz NOT NULL,
    sport_type text NOT NULL,
    distance_m numeric,
    comment text,
    source text DEFAULT 'simulation',
    created_at timestamptz DEFAULT now(),
    CONSTRAINT ck_activity_dates CHECK (end_at > start_at),
    CONSTRAINT ck_distance_non_negative CHECK (distance_m IS NULL OR distance_m >= 0)
);

CREATE INDEX IF NOT EXISTS idx_activities_employee_start
    ON raw.activities(employee_id, start_at DESC);

CREATE TABLE IF NOT EXISTS monitoring.pipeline_events (
    event_id bigserial PRIMARY KEY,
    component text NOT NULL,
    status text NOT NULL,
    details text,
    created_at timestamptz DEFAULT now()
);

-- La table raw.activities est capturée par Debezium via le WAL PostgreSQL.
-- La publication est créée automatiquement par le connecteur Debezium en mode filtered.
