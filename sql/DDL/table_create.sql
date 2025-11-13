
CREATE TABLE IF NOT EXISTS raw_pupil (
    SnapshotDate   TEXT,
    AcademyName    TEXT,
    PupilID        TEXT,
    FirstName      TEXT,
    MiddleName     TEXT,
    LastName       TEXT,
    Status         TEXT,
    GradeId        TEXT,
    GradeName      TEXT,
    Stream         TEXT
);

CREATE TABLE IF NOT EXISTS raw_pupil_attendance (
    Date       TEXT,
    PupilID    TEXT,
    Attendance TEXT
);

CREATE TABLE IF NOT EXISTS stg_pupil (
    snapshot_date DATE NOT NULL,
    pupil_id INTEGER NOT NULL,
    academy_name TEXT,
    first_name TEXT,
    last_name TEXT,
    status TEXT,
    grade_id INTEGER,
    grade_name TEXT,
    stream TEXT,
    PRIMARY KEY (pupil_id, snapshot_date)
);


CREATE TABLE IF NOT EXISTS stg_pupil_attendance (
    attendance_date DATE NOT NULL,
    pupil_id INTEGER NOT NULL,
    attendance TEXT,
    PRIMARY KEY (pupil_id, attendance_date)
);

--DROP TABLE IF EXISTS dim_date;
CREATE TABLE IF NOT EXISTS dim_date (
    date_key   INTEGER PRIMARY KEY,   
    date       DATE NOT NULL,        
    year       INTEGER,
    month      INTEGER,
    day        INTEGER,
    weekday    TEXT
);

--DROP TABLE IF EXISTS dim_pupil;
CREATE TABLE IF NOT EXISTS dim_pupil (
    pupil_key   INTEGER PRIMARY KEY,     
    pupil_id    INTEGER UNIQUE NOT NULL, 
    first_name  TEXT,
    last_name   TEXT
);

--DROP TABLE IF EXISTS dim_academy;
CREATE TABLE IF NOT EXISTS dim_academy (
    academy_key INTEGER PRIMARY KEY,
    academy_name TEXT UNIQUE NOT NULL
);

--DROP TABLE IF EXISTS dim_grade;
CREATE TABLE IF NOT EXISTS dim_grade (
    grade_key INTEGER PRIMARY KEY,
    grade_id INTEGER UNIQUE NOT NULL,
    grade_name TEXT NOT NULL
);

--DROP TABLE IF EXISTS dim_stream;
CREATE TABLE IF NOT EXISTS dim_stream (
    stream_key INTEGER PRIMARY KEY,
    stream TEXT UNIQUE NOT NULL
);

--DROP TABLE IF EXISTS fact_pupil_attendance_daily;
CREATE TABLE IF NOT EXISTS fact_pupil_attendance_daily (
    fact_date     DATE NOT NULL,          
    date_key      INTEGER NOT NULL,
    pupil_key     INTEGER NOT NULL,
    academy_key   INTEGER,
    grade_key     INTEGER,
    stream_key    INTEGER,
    status        TEXT,
    attendance    TEXT,
    prcs_id TEXT,
    PRIMARY KEY (fact_date, pupil_key),   
    FOREIGN KEY (date_key)    REFERENCES dim_date(date_key),
    FOREIGN KEY (pupil_key)   REFERENCES dim_pupil(pupil_key),
    FOREIGN KEY (academy_key) REFERENCES dim_academy(academy_key),
    FOREIGN KEY (grade_key)   REFERENCES dim_grade(grade_key),
    FOREIGN KEY (stream_key)  REFERENCES dim_stream(stream_key)
);