# --------------------------------------------------------------
#  pupil_datamart_to_postgres.py
# --------------------------------------------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.operators.bash import BashOperator
from datetime import datetime
from io import StringIO
import pandas as pd
import logging

# ------------------------------------------------------------------
SQLITE_CONN_ID   = 'sqlite_local'     
POSTGRES_CONN_ID = 'postgres_target' 

DBT_PROJECT_DIR = '/mnt/c/Users/PC/Documents/newglobe-pupil-datamart/pupil_dbt'  # Path to your DBT project root (e.g., where dbt_project.yml is)
DBT_PROFILES_DIR = '/home/simmie09/.dbt'  # Path to ~/.dbt/profiles.yml (standard)

TABLES_TO_SYNC = [
    'fact_pupil_attendance_daily',
    'dim_date',
    'dim_pupil',
    'dim_academy',
    'dim_grade',
    'dim_stream',
]

# ------------------------------------------------------------------
with DAG(
    dag_id='pupil_datamart_to_postgres',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',          
    catchup=False,
    tags=['sync', 'postgres', 'pupil'],
    description='Daily full-refresh SQLite → Postgres',
    default_args={'retries': 1},
) as dag:

    def _sqlite_to_pg_type(sqlite_type: str) -> str:
        sqlite_type = sqlite_type.upper()
        if sqlite_type.startswith('INTEGER'):   return 'INTEGER'
        if sqlite_type.startswith('TEXT'):      return 'TEXT'
        if sqlite_type.startswith('REAL'):      return 'DOUBLE PRECISION'
        if sqlite_type.startswith('NUMERIC'):   return 'NUMERIC'
        if sqlite_type.startswith('BLOB'):      return 'BYTEA'
        return 'TEXT'

    def sync_table(table_name: str, **context):
        # Read prcs_id from upstream conf
        prcs_id = context['dag_run'].conf.get('prcs_id')
        if prcs_id:
            logging.info("Using prcs_id: %s for sync", prcs_id)
        else:
            logging.warning("No prcs_id in conf; syncing full table")

        # Build SELECT with optional WHERE for fact table only
        where_clause = ""
        if table_name == 'fact_pupil_attendance_daily' and prcs_id:
            where_clause = f" WHERE prcs_id = '{prcs_id}'"

        select_sql = f'SELECT * FROM "{table_name}"{where_clause}'

        # Extract data from SQLite tables
        sqlite_hook = SqliteHook(sqlite_conn_id=SQLITE_CONN_ID)
        logging.info("Fetching %s from SQLite (query: %s)...", table_name, select_sql)
        df = sqlite_hook.get_pandas_df(sql=select_sql)

        if df.empty:
            logging.warning("%s is empty, nothing to copy.", table_name)
            return
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cur  = conn.cursor()

        try:
            # Build CREATE TABLE statement from DataFrame dtypes
            col_defs = []
            for col, dtype in df.dtypes.items():
                pg_type = _sqlite_to_pg_type(str(dtype))
                col_defs.append(f'"{col}" {pg_type}')

            # Updated: DROP before CREATE to ensure schema matches current DF (handles new columns like prcs_id)
            drop_sql = f'DROP TABLE IF EXISTS public."{table_name}"'
            cur.execute(drop_sql)

            create_sql = f'''
                CREATE TABLE public."{table_name}" (
                    {", ".join(col_defs)}
                )
            '''
            cur.execute(create_sql)

            if not df.empty:
                # CSV in memory
                output = StringIO()
                # Use \N for NULL (Postgres COPY default)
                df.to_csv(
                    output,
                    sep='\t',
                    header=False,
                    index=False,
                    na_rep='\\N',
                    quoting=None,        
                )
                output.seek(0)

                copy_sql = f'''
                    COPY public."{table_name}" FROM STDIN
                    WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N', HEADER FALSE)
                '''
                cur.copy_expert(copy_sql, output)

            conn.commit()

            logging.info(
                "Synced %s rows → public.%s", len(df), table_name
            )

        except Exception as e:
            conn.rollback()
            logging.exception("Failed to sync %s", table_name)
            raise
        finally:
            cur.close()
            conn.close()

    sync_tasks = [
        PythonOperator(
            task_id=f'sync_{tbl}',
            python_callable=sync_table,
            op_kwargs={'table_name': tbl},
            provide_context=True,  # Enables **context for conf access
        )
        for tbl in TABLES_TO_SYNC
    ]

    for i in range(len(sync_tasks) - 1):
        sync_tasks[i] >> sync_tasks[i + 1]


    run_dbt = BashOperator(
        task_id='run_dbt_transforms',
        bash_command=f'''
            cd {DBT_PROJECT_DIR} &&
            dbt run --select marts --profiles-dir {DBT_PROFILES_DIR} &&
            dbt test --select marts --profiles-dir {DBT_PROFILES_DIR}
        ''',
        cwd=DBT_PROJECT_DIR
    )

    sync_tasks[-1] >> run_dbt