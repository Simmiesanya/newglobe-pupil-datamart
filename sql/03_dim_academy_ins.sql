INSERT OR IGNORE INTO dim_academy (academy_name)
SELECT academy_name
FROM (
    SELECT DISTINCT academy_name
    FROM stg_pupil
    WHERE academy_name IS NOT NULL
) AS distinct_academies
ORDER BY academy_name;