{{ config(materialized='table') }}  -- Materialize as table for Power BI performance

WITH daily_attendance AS (
    SELECT
        f.fact_date,
        a.academy_name,
        g.grade_name,
        SUM(CASE WHEN f.attendance = 'Present' THEN 1.0 ELSE 0.0 END) / COUNT(*) AS attendance_rate,
        COUNT(*) AS total_pupils_presented
    FROM {{ source('postgres', 'fact_pupil_attendance_daily') }} f
    LEFT JOIN {{ source('postgres', 'dim_academy') }} a ON f.academy_key = a.academy_key
    LEFT JOIN {{ source('postgres', 'dim_grade') }} g ON f.grade_key = g.grade_key
    GROUP BY f.fact_date, a.academy_name, g.grade_name
)

SELECT
    d.date AS fact_date,  -- Enriched with dim_date
    da.academy_name,
    da.grade_name,
    da.attendance_rate,
    da.total_pupils_presented,
    d.year,
    d.month,
    d.weekday
FROM daily_attendance da
JOIN {{ source('postgres', 'dim_date') }} d ON da.fact_date = d.date
ORDER BY d.date DESC, da.academy_name