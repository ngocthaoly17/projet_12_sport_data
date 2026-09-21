CREATE SCHEMA IF NOT EXISTS analytics;


-- =========================================================
-- 1. PARAMETRES DES BENEFICES
-- =========================================================

CREATE TABLE IF NOT EXISTS analytics.benefit_parameters (
    parameter_name  text,
    parameter_value numeric,
    valid_from      date,
    valid_to        date,

    PRIMARY KEY (parameter_name, valid_from)
);


-- Insère les paramètres uniquement s'il n'existe
-- pas déjà une version active (valid_to IS NULL).

INSERT INTO analytics.benefit_parameters (
    parameter_name,
    parameter_value,
    valid_from,
    valid_to
)
SELECT
    v.name,
    v.value,
    current_date,
    NULL
FROM (
    VALUES
        ('bonus_rate',               0.05::numeric),
        ('wellness_days',            5::numeric),
        ('min_annual_activities',   15::numeric),
        ('max_walk_distance_km',    15::numeric),
        ('max_cycle_distance_km',   25::numeric)
) AS v(name, value)

WHERE NOT EXISTS (
    SELECT 1
    FROM analytics.benefit_parameters p
    WHERE p.parameter_name = v.name
      AND p.valid_to IS NULL
);


-- =========================================================
-- 2. ACTIVITES DES EMPLOYES SUR LES 12 DERNIERS MOIS
-- =========================================================

CREATE OR REPLACE VIEW analytics.v_employee_activity_12m AS

SELECT
    e.employee_id,
    e.first_name,
    e.last_name,

    COUNT(a.activity_id) AS activity_count_12m,

    COALESCE(
        SUM(a.distance_m),
        0
    ) AS distance_m_12m

FROM raw.employees e

LEFT JOIN raw.activities a
    ON a.employee_id = e.employee_id
   AND a.start_at >= now() - interval '12 months'

GROUP BY
    e.employee_id,
    e.first_name,
    e.last_name;


-- =========================================================
-- 3. CALCUL DE L'ELIGIBILITE AUX BENEFICES
-- =========================================================

DROP VIEW IF EXISTS analytics.v_benefit_eligibility;


CREATE VIEW analytics.v_benefit_eligibility AS

WITH params AS (

    SELECT

        MAX(parameter_value) FILTER (
            WHERE parameter_name = 'min_annual_activities'
              AND valid_to IS NULL
        ) AS min_activities,

        MAX(parameter_value) FILTER (
            WHERE parameter_name = 'wellness_days'
              AND valid_to IS NULL
        ) AS wellness_days,

        MAX(parameter_value) FILTER (
            WHERE parameter_name = 'bonus_rate'
              AND valid_to IS NULL
        ) AS bonus_rate

    FROM analytics.benefit_parameters
)


SELECT

    -- Informations employé
    e.employee_id,
    e.first_name,
    e.last_name,
    e.annual_gross_salary,
    e.commute_mode,

    -- Nombre d'activités sur les 12 derniers mois
    v.activity_count_12m,


    -- =====================================================
    -- WELLNESS DAYS
    -- =====================================================

    (
        v.activity_count_12m >= p.min_activities
    ) AS wellness_eligible,


    CASE

        WHEN v.activity_count_12m >= p.min_activities
        THEN p.wellness_days

        ELSE 0

    END AS wellness_days,


    -- =====================================================
    -- PRIME TRANSPORT / SPORT
    --
    -- Eligible si :
    -- 1. mode physique (marche/running ou vélo...)
    -- 2. trajet validé par commute_validation
    --
    -- U&'\00E9' représente le caractère "é".
    -- Cela évite les problèmes d'encodage du fichier SQL.
    -- =====================================================

    (
        e.commute_mode IN (
            'Marche/running',
            U&'V\00E9lo/Trottinette/Autres'
        )
        AND cv.validation_status = 'VALID'

    ) AS bonus_eligible,


    -- =====================================================
    -- MONTANT DE LA PRIME
    -- 5 % du salaire annuel brut si éligible
    -- =====================================================

    CASE

        WHEN
            e.commute_mode IN (
                'Marche/running',
                U&'V\00E9lo/Trottinette/Autres'
            )
            AND cv.validation_status = 'VALID'

        THEN ROUND(
            e.annual_gross_salary * p.bonus_rate,
            2
        )

        ELSE 0

    END AS bonus_amount


FROM raw.employees e

JOIN analytics.v_employee_activity_12m v
    USING (employee_id)

LEFT JOIN monitoring.commute_validation cv
    USING (employee_id)

CROSS JOIN params p;