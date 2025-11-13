{{ config(materialized='table') }}

WITH academy_metrics AS (
    SELECT
        a.academy_name,
        AVG(CASE WHEN f.attendance = 'Present' THEN 1.0 ELSE 0.0 END) AS overall_attendance_rate,
        COUNT(DISTINCT f.pupil_key) AS total_unique_pupils
    FROM {{ source('postgres', 'fact_pupil_attendance_daily') }} f
    LEFT JOIN {{ source('postgres', 'dim_academy') }} a ON f.academy_key = a.academy_key
    GROUP BY a.academy_name
)

SELECT
    am.academy_name,
    am.overall_attendance_rate,
    am.total_unique_pupils,
    RANK() OVER (ORDER BY am.overall_attendance_rate DESC) AS rank
FROM academy_metrics am
ORDER BY rank