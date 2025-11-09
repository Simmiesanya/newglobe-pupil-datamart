DELETE FROM fact_pupil_attendance_daily;

DELETE FROM raw_pupil;
DELETE FROM raw_pupil_attendance;

DELETE FROM stg_pupil;
DELETE FROM stg_pupil_attendance;

DELETE FROM dim_date;
DELETE FROM dim_pupil;
DELETE FROM dim_academy;
DELETE FROM dim_grade;
DELETE FROM dim_stream;


DELETE FROM sqlite_sequence WHERE name IN (
    'dim_date', 'dim_pupil', 'dim_academy', 'dim_grade', 'dim_stream'
);