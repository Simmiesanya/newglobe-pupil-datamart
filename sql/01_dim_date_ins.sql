INSERT OR IGNORE INTO dim_date (date_key, date, year, month, day, weekday)
SELECT 
    CAST(REPLACE(snapshot_date, '-', '') AS INTEGER),
    DATE(snapshot_date),
    CAST(strftime('%Y', snapshot_date) AS INTEGER),
    CAST(strftime('%m', snapshot_date) AS INTEGER),
    CAST(strftime('%d', snapshot_date) AS INTEGER),
    CASE CAST(strftime('%w', snapshot_date) AS INTEGER)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END
FROM (
    SELECT DISTINCT snapshot_date 
    FROM stg_pupil
) AS distinct_dates
ORDER BY snapshot_date;