INSERT OR IGNORE INTO dim_pupil (pupil_id, first_name, last_name)
SELECT 
    pupil_id,
    first_name,
    last_name
FROM (
    SELECT DISTINCT 
        pupil_id, 
        first_name, 
        last_name
    FROM stg_pupil
) AS distinct_pupils
ORDER BY pupil_id;