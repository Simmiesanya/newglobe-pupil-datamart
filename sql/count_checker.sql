SELECT table_name, row_count
FROM (
  SELECT 'raw_pupil' AS table_name, COUNT(*) AS row_count FROM raw_pupil
  UNION ALL
  SELECT 'raw_pupil_attendance', COUNT(*) FROM raw_pupil_attendance
  UNION ALL
  SELECT 'stg_pupil', COUNT(*) FROM stg_pupil
  UNION ALL
  SELECT 'stg_pupil_attendance', COUNT(*) FROM stg_pupil_attendance
  UNION ALL
  SELECT 'dim_date', COUNT(*) FROM dim_date
  UNION ALL
  SELECT 'dim_pupil', COUNT(*) FROM dim_pupil
  UNION ALL
  SELECT 'dim_academy', COUNT(*) FROM dim_academy
  UNION ALL
  SELECT 'dim_grade', COUNT(*) FROM dim_grade
  UNION ALL
  SELECT 'dim_stream', COUNT(*) FROM dim_stream
  UNION ALL
  SELECT 'fact_pupil_attendance_daily', COUNT(*) FROM fact_pupil_attendance_daily
) AS data
ORDER BY
  CASE 
    WHEN table_name LIKE 'raw_%' THEN 1
    WHEN table_name LIKE 'stg_%' THEN 2
    WHEN table_name LIKE 'dim_%' THEN 3
    WHEN table_name LIKE 'fact_%' THEN 4
    ELSE 5
  END,
  table_name