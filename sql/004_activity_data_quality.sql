-- =========================================================
-- DATA QUALITY - ACTIVITES SPORTIVES
-- =========================================================

CREATE TABLE IF NOT EXISTS monitoring.activity_dq_results (
    activity_id  bigint,
    employee_id  integer,
    rule_name    text,
    error_message text,
    detected_at  timestamptz DEFAULT now()
);


-- On recalcule les anomalies à chaque exécution
TRUNCATE TABLE monitoring.activity_dq_results;


-- =========================================================
-- DQ01 - DISTANCE NEGATIVE
-- =========================================================

INSERT INTO monitoring.activity_dq_results
SELECT
    activity_id,
    employee_id,
    'DQ01_NEGATIVE_DISTANCE',
    U&'La distance est n\00E9gative.',
    now()
FROM raw.activities
WHERE distance_m < 0;


-- =========================================================
-- DQ02 - DUREE INVALIDE
-- La fin doit être postérieure au début
-- =========================================================

INSERT INTO monitoring.activity_dq_results
SELECT
    activity_id,
    employee_id,
    'DQ02_INVALID_DURATION',
    U&'La date de fin est ant\00E9rieure ou \00E9gale \00E0 la date de d\00E9but.',
    now()
FROM raw.activities
WHERE end_at <= start_at;


-- =========================================================
-- DQ03 - SPORT MANQUANT
-- =========================================================

INSERT INTO monitoring.activity_dq_results
SELECT
    activity_id,
    employee_id,
    'DQ03_MISSING_SPORT',
    'Le type de sport est vide.',
    now()
FROM raw.activities
WHERE sport_type IS NULL
   OR btrim(sport_type) = '';


-- =========================================================
-- DQ04 - EMPLOYE INCONNU
-- L'activité référence un employee_id inexistant
-- =========================================================

INSERT INTO monitoring.activity_dq_results
SELECT
    a.activity_id,
    a.employee_id,
    'DQ04_UNKNOWN_EMPLOYEE',
    U&'L''activit\00E9 r\00E9f\00E9rence un employ\00E9 inexistant.',
    now()
FROM raw.activities a

LEFT JOIN raw.employees e
    ON e.employee_id = a.employee_id

WHERE e.employee_id IS NULL;


-- =========================================================
-- DQ05 - ACTIVITE DANS LE FUTUR
-- =========================================================

INSERT INTO monitoring.activity_dq_results
SELECT
    activity_id,
    employee_id,
    'DQ05_FUTURE_ACTIVITY',
    U&'La date de d\00E9but est dans le futur.',
    now()
FROM raw.activities
WHERE start_at > now();


-- =========================================================
-- DQ06 - DISTANCE INATTENDUE
--
-- Ces sports sont considérés comme non kilométriques.
-- Une distance ne devrait donc normalement pas être renseignée.
-- =========================================================

INSERT INTO monitoring.activity_dq_results
SELECT
    activity_id,
    employee_id,
    'DQ06_UNEXPECTED_DISTANCE',

    U&'Distance renseign\00E9e pour un sport non kilom\00E9trique : '
        || sport_type || '.',

    now()

FROM raw.activities

WHERE distance_m IS NOT NULL

AND sport_type IN (
    'Judo',
    'Boxe',
    'Tennis',
    'Badminton',
    'Tennis de table',
    'Basketball',
    'Football',
    'Rugby',
    'Escalade',
    U&'\00C9quitation',
    'Natation',
    'Voile'
);


-- =========================================================
-- DQ07 - DISTANCE MANQUANTE
--
-- Pour ces sports kilométriques, une distance est attendue.
-- =========================================================

INSERT INTO monitoring.activity_dq_results
SELECT
    activity_id,
    employee_id,
    'DQ07_MISSING_DISTANCE',

    U&'Distance manquante pour un sport kilom\00E9trique : '
        || sport_type || '.',

    now()

FROM raw.activities

WHERE distance_m IS NULL

AND sport_type IN (
    'Marche',
    'Running',
    U&'Randonn\00E9e',
    'Triathlon'
);


-- =========================================================
-- DQ08 - NOM DE SPORT INVALIDE
--
-- "Runing" est considéré comme une faute de frappe.
-- La valeur correcte est "Running".
-- =========================================================

INSERT INTO monitoring.activity_dq_results
SELECT
    activity_id,
    employee_id,
    'DQ08_INVALID_SPORT_NAME',

    U&'Le type de sport "Runing" doit \00EAtre normalis\00E9 en "Running".',

    now()

FROM raw.activities

WHERE sport_type = 'Runing';


-- =========================================================
-- VUE DE SYNTHESE
-- Nombre d'anomalies détectées par règle DQ
-- =========================================================

CREATE OR REPLACE VIEW analytics.v_activity_dq_summary AS

SELECT
    rule_name,
    COUNT(*) AS anomaly_count

FROM monitoring.activity_dq_results

GROUP BY rule_name;