{{ config(materialized='table') }}

SELECT
    ar.weekday,
    AVG(1 - ar.attendance_rate) AS avg_absenteeism_rate,  -- Invert for absences
    AVG(ar.total_pupils_presented) AS avg_daily_pupils,
    COUNT(DISTINCT ar.academy_name) AS academies_count
FROM {{ ref('attendance_rate') }} ar  -- Reuses the base model
GROUP BY ar.weekday
ORDER BY avg_absenteeism_rate DESC  -- Prioritize high-absence days