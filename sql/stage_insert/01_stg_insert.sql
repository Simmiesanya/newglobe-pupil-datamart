-- STAGE: PUPIL
INSERT OR IGNORE INTO stg_pupil
SELECT
    DATE(SUBSTR(SnapshotDate,1,4)||'-'||SUBSTR(SnapshotDate,6,2)||'-'||SUBSTR(SnapshotDate,9,2)),
    CAST(PupilID AS INTEGER),
    NULLIF(TRIM(AcademyName),''),
    NULLIF(TRIM(FirstName),''),
    NULLIF(TRIM(LastName),''),
    NULLIF(TRIM(Status),''),
    CAST(GradeId AS INTEGER),
    CASE
        WHEN TRIM(UPPER(GradeName)) IN ('PRIMARY1','PRIMARY 1') THEN 'Primary 1'
        WHEN TRIM(UPPER(GradeName)) IN ('PRIMARY2','PRIMARY 2') THEN 'Primary 2'
        WHEN TRIM(UPPER(GradeName)) IN ('PRIMARY3','PRIMARY 3') THEN 'Primary 3'
        WHEN TRIM(UPPER(GradeName)) IN ('PRIMARY4','PRIMARY 4') THEN 'Primary 4'
        WHEN TRIM(UPPER(GradeName)) IN ('PRIMARY5','PRIMARY 5') THEN 'Primary 5'
        WHEN TRIM(UPPER(GradeName)) IN ('PRIMARY6','PRIMARY 6') THEN 'Primary 6'
        WHEN TRIM(UPPER(GradeName)) IN ('PRIMARY7','PRIMARY 7') THEN 'Primary 7'
        WHEN TRIM(UPPER(GradeName))='BABY CLASS'   THEN 'Baby Class'
        WHEN TRIM(UPPER(GradeName))='MIDDLE CLASS' THEN 'Middle Class'
        WHEN TRIM(UPPER(GradeName))='TOP CLASS'    THEN 'Top Class'
        ELSE NULL
    END,
    CASE WHEN Stream IS NULL OR TRIM(Stream)='' THEN NULL ELSE UPPER(TRIM(Stream)) END
FROM raw_pupil;

-- STAGE: ATTENDANCE
INSERT OR IGNORE INTO stg_pupil_attendance
SELECT
    DATE(SUBSTR(Date,1,4)||'-'||SUBSTR(Date,6,2)||'-'||SUBSTR(Date,9,2)),
    CAST(NULLIF(TRIM(PupilID),'') AS INTEGER),
    NULLIF(TRIM(Attendance),'')
FROM raw_pupil_attendance;