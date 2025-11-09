School Attendance Data Mart ETL Framework

NEWGLOBE-PUPIL-DATAMART/ ├── dags/
│ └── pupil_datamart_dag.py ├── data/
│ ├── PupilAttendance.csv │ └── PupilData.csv ├── pupil_env/
├── sql/ │ └── DDL/
│ └── table_create.sql │ └── stage_insert/
│ ├── 01_stg_insert.sql │ ├── 01_dim_date_ins.sql │ ├── 02_dim_pupil_ins.sql │ ├── 03_dim_academy_ins.sql │ ├── 04_dim_grade_ins.sql │ ├── 05_dim_stream_ins.sql │ ├── 06_fact_att_ins.sql │ ├── count_checker.sql │ ├── drop_tables.sql │ ├── truncate_tables.sql │ ├── load_raw.py │ └── newglobe_pupil_datamart.db └── README.md

Source files: pupil_data.csv and attendance_data.csv

Raw Layer (Truncate & Load): The raw layer consists of two tables raw_pupil and raw_pupil_attendance. Both tables are loaded directly from the source csv files (pupil_data.csv and attendance_data.csv) in each dag run. The raw layer does not keep data history, so each load is a full refreshe based on the content of the csv files. I decided to use a trunc and load because CSVs are full refreshes, so no deltas to worry about. Pandas reads the CSVs and inserts via SQL.

Staging Layer (Insert-Only): The raw layer consists of two tables stg_pupil and stg_pupil_attendance. Both tables are populated with data from the raw tables in each dag run. The stage tables keep history and they're insert only, so no duplicates are kept in the table Originally, i opted for md5 hashing but sqlite presented alot of limitations for MD5() or SHA256() for hashing rows, so i went for insert or ignore. Column -by-column detection would have been an option but for performance wise, it is not the best solution for data insertion, even though it guarantees deduplication

Dimension Layers (Insert or Ignore):

Dim date: A date column that carries the range of dates found in the pupil table based on snapshot and attendance date
Dim pupil: Shows the distinct list of students
Dim academy: Shows the list of academies based on the pupil data
Dim grade: Shows the grades
Dim stream: Shows the streams The tables mentioned above are also insert or ignore When a new row arrives from the stage table, it is inserted into the appropriate dim tables Logic: INSERT OR IGNORE on the natural key (e.g., academy_name). If new, insert with a new *_key (auto-increment). If exists, reuse the key. This handles inserts/updates implicitly without full SCD complexity. Assumption: Dimensions change rarely; we don't track history here (e.g., if a pupil's name changes, it updates in dim_pupil via stage changes).
Fact Table (Append-Only): This is the attendance sheet. It is an append only table and no existing record is updated, new records are only inserted
fact_pupil_attendance_daily: Joins stage/ dims on keys, appends new rows with measures like attendance_status (mapped to 1/0 or codes).
Foreign keys enforced (with PRAGMA foreign_keys = ON after loads).
Assumption: Facts are additive; we append daily snapshots without aggregation here (do that in queries).
Incremtal load logic: While the CSV is a static data, i realized data was written into it on a daily basis so i modeled my pipeline to run daily based on the snapshot date in the stg pupil table. This mimics a daily data ingestion model

Truncate raw tables
Load CSVs into raw_pupil & raw_pupil_attendance
Insert into stage → only new/changed pupil-day combos
Insert into dims → only new values
Insert into fact → only the NEXT day after last loaded The fact load also has a fail safe logic. If it is the first load, meaning the fact table is empty, the code looks at the dim_date table and picks the MIn date there, this is to ensure that our incremental load starts from the very begining. However, if the fact table is not empty, the script fetches the MAX date in the fact table and then loads data for that date +1 day, so it'll always be a da=eltsa load of 1 day
Full Flow

ETL Script (pupil_datamart_dag.py): Orchestrates everything in one Python file. Uses sqlite3 for DB ops and pandas for CSV reads.
Scheduling Script (run_etl.sh): The framework uses airflow cron scheduler and the dag is currently set to run hourly
DB Schema: All in sql/DDL/table_create.sql
Data Volume Assumption: Small-scale (thousands of rows). SQLite handles it fine.
Date Handling: Assumes CSVs have dates in YYYY/MM/DD, I normalized to YYYY-MM-DD.
Bonus: sql scripts to truncate all tables, drop all tables, get the row count of all tables
Challenges & Trade-offs

SQLite Limits: While dbbrowser is great for dev, the absense of native hashing slowed stage change detection.

Windows Limitation: Airflow isn't compatible with Windows natively. I dual-booted into Ubuntu WSL2, which was a headache (installing dependencies, managing venvs).

Running the dag python script: OPTION 1: Run Directly with Python Step 1: Download & Unzip Extract the project folder: NEWGLOBE-PUPIL-DATAMART

Step 2: Open Windows Terminal (CMD or PowerShell) navigate to cd C:\path\to\NEWGLOBE-PUPIL-DATAMART

Step 3: Install Dependencies pip install pandas

Step 4: Run the ETL python dags\pupil_datamart_dag.py

OPTION 2: Run with Airflow Step 1: Install WSL2 (Ubuntu) Open PowerShell as Admin Run: powershellwsl --install -d Ubuntu Restart your PC Open Ubuntu app → set username/password

Step 2: Open WSL Terminal

In WSL terminal, type this or the appropriate path the folder is saved
cd /mnt/c/path/to/NEWGLOBE-PUPIL-DATAMART

Step 3: Install Airflow & Dependencies pip install "apache-airflow[pandas,sqlite]"

Step 4: Set Up Airflow export AIRFLOW_HOME=~/airflow mkdir -p $AIRFLOW_HOME airflow db init

Step 5: Create Admin User airflow users create
--username admin
--password admin
--firstname Admin
--lastname User
--role Admin
--email admin@example.com

Step 6: Copy DAG cp dags/pupil_datamart_dag.py ~/airflow/dags/

Step 7: Start Airflow

Terminal 1 (WSL)
airflow scheduler

Terminal 2 (WSL)
airflow webserver --port 8080

Step 8: Open Web UI Go to: http://localhost:8080 Login: admin / admin → Find pupil_datamart_dag → Turn ON → Trigger

How to Query the Database STEP 1: Using Python (Built-in) python -c "import sqlite3; conn = sqlite3.connect('sql/DDL/newglobe_pupil_datamart.db'); print('Total pupils:', conn.execute('SELECT COUNT(*) FROM dim_pupil').fetchone()[0]); conn.close()"

STEP 2: Using DB Browser (GUI – Optional) Download: https://sqlitebrowser.org/ Open newglobe_pupil_datamart.db Go to Execute SQL → run any query

NB: do not leave the database open in db browser when running the dag or when running a query from terminal. Always close database first.

Ad-hoc SQL Runner: run_sql_adhoc What it does: Runs any .sql file from your sql/ folder Use cases:

count_checker.sql → Show row counts
truncate_tables.sql → Reset data
drop_tables.sql → Full wipe
Any custom query
How to use (UI): DAGs → run_sql_adhoc → Trigger DAG {"sql_file": "C:/Users/PC/Documents/newglobe-pupil-datamart/sql/count_checker.sql"} Click Trigger

How to use (Terminal): bashairflow dags trigger run_sql_adhoc
-c '{"sql_file": "/mnt/c/Users/PC/Documents/newglobe-pupil-datamart/sql/count_checker.sql"}'
