INSERT OR IGNORE INTO fact_pupil_attendance_daily
SELECT 
    sp.snapshot_date AS fact_date,
    d.date_key,
    p.pupil_key,
    a.academy_key,
    g.grade_key,
    s.stream_key,
    sp.status,
    sa.attendance
FROM (
    SELECT 
        CASE 
            WHEN (SELECT COUNT(*) FROM fact_pupil_attendance_daily) = 0
            THEN (SELECT MIN(date) FROM dim_date)
            ELSE date((SELECT MAX(fact_date) FROM fact_pupil_attendance_daily), '+1 day')
        END AS target_date
) AS config
JOIN stg_pupil sp ON sp.snapshot_date = config.target_date
LEFT JOIN stg_pupil_attendance sa 
    ON sa.attendance_date = sp.snapshot_date 
    AND sa.pupil_id = sp.pupil_id
JOIN dim_date d ON d.date = sp.snapshot_date
JOIN dim_pupil p ON p.pupil_id = sp.pupil_id
LEFT JOIN dim_academy a ON a.academy_name = sp.academy_name
LEFT JOIN dim_grade g ON g.grade_id = sp.grade_id
LEFT JOIN dim_stream s ON s.stream = sp.stream
WHERE config.target_date IS NOT NULL
  AND EXISTS (SELECT 1 FROM stg_pupil WHERE snapshot_date = config.target_date);
  
SELECT 
    'INCREMENTAL LOAD RESULT' AS status,
    (SELECT COUNT(*) FROM fact_pupil_attendance_daily) AS total_fact_rows,
    (SELECT MAX(fact_date) FROM fact_pupil_attendance_daily) AS last_loaded_date,
    CHANGES() AS rows_inserted_this_run;