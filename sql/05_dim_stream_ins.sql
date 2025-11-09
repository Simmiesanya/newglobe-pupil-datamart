INSERT OR IGNORE INTO dim_stream (stream)
SELECT DISTINCT stream
FROM stg_pupil
WHERE stream IS NOT NULL;