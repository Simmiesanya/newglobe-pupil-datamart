INSERT OR IGNORE INTO dim_grade (grade_id, grade_name)
SELECT DISTINCT grade_id, grade_name
FROM stg_pupil
WHERE grade_id IS NOT NULL AND grade_name IS NOT NULL;