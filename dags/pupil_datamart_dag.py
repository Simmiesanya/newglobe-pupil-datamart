from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import sqlite3
from airflow.operators.python import PythonOperator 
import sys
import hashlib
from jinja2 import Template
from datetime import datetime

# ========================================
# CHANGE THIS ONE LINE TO SUIT YOUR FOLDER PATH
# ========================================
BASE_PATH = r"C:/Users/PC/Documents/newglobe-pupil-datamart" 
# ========================================

WSL_PATH = BASE_PATH.replace("C:", "/mnt/c").replace("\\", "/")
SQL_DIR = f"{WSL_PATH}/sql"
PROJECT_ROOT = WSL_PATH
DB_PATH = f"{WSL_PATH}/newglobe_pupil_datamart.db"

def generate_prcs_id(**kwargs):
    ti = kwargs['ti']
    now = datetime.utcnow()
    timestamp_str = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  
    prcs_id = hashlib.md5(timestamp_str.encode()).hexdigest()
    ti.xcom_push(key="prcs_id", value=prcs_id)
    print(f"Generated prcs_id: {prcs_id} from timestamp: {timestamp_str}")

# def run_sql_file(sql_path):
#     with sqlite3.connect(DB_PATH) as conn:
#         with open(sql_path, 'r') as f:
#             sql = f.read()
#         conn.executescript(sql)

def run_sql_file(sql_path, **context):
    with sqlite3.connect(DB_PATH) as conn:
        with open(sql_path, 'r') as f:
            template_str = f.read()
        template = Template(template_str)
        sql = template.render(**context)  # Renders {{ }} with task_instance, etc.
        
        # Debug: Print rendered SQL to task logs (remove after testing)
        print("Rendered SQL preview:")
        print(repr(sql[:200]))  # First 200 chars; adjust if needed
        print("..." if len(sql) > 200 else "")

        conn.executescript(sql)

with DAG(
    dag_id='pupil_datamart_daily',
    default_args={'owner': 'simmie09', 'retries': 1},
    description='Daily ETL: CSV → raw → stage → dim → fact',
    schedule_interval=None,  # '*/10 * * * *',  # every 10 minutes for testing
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['pupil'],
    max_active_runs=1,
    template_searchpath=SQL_DIR,
) as dag:

    start = PythonOperator(
        task_id='start',
        python_callable=generate_prcs_id,
    )

    create_tables = PythonOperator(
        task_id='create_tables',
        python_callable=run_sql_file,
        op_kwargs={'sql_path': f'{WSL_PATH}/sql/DDL/table_create.sql'},
    )

    load_csv_to_raw = BashOperator(
        task_id='load_csv_to_raw',
        bash_command=f'"{sys.executable}" "{PROJECT_ROOT}/sql/load_raw.py"',
        cwd=PROJECT_ROOT
    )

    load_raw_to_stage = PythonOperator(
        task_id='load_raw_to_stage',
        python_callable=run_sql_file,
        op_kwargs={'sql_path': f'{WSL_PATH}/sql/stage_insert/01_stg_insert.sql'},
    )
    load_dims_date = PythonOperator(
        task_id='load_dim_date',
        python_callable=run_sql_file,
        op_kwargs={'sql_path': f'{WSL_PATH}/sql/01_dim_date_ins.sql'},
    )
    load_dim_pupil = PythonOperator(
        task_id='load_dim_pupil',
        python_callable=run_sql_file,
        op_kwargs={'sql_path': f'{WSL_PATH}/sql/02_dim_pupil_ins.sql'},
    )
    load_dim_academy = PythonOperator(
        task_id='load_dim_academy',
        python_callable=run_sql_file,
        op_kwargs={'sql_path': f'{WSL_PATH}/sql/03_dim_academy_ins.sql'},
    )
    load_dim_grade = PythonOperator(
        task_id='load_dim_grade',
        python_callable=run_sql_file,
        op_kwargs={'sql_path': f'{WSL_PATH}/sql/04_dim_grade_ins.sql'},
    )
    load_dim_stream = PythonOperator(
        task_id='load_dim_stream',
        python_callable=run_sql_file,
        op_kwargs={'sql_path': f'{WSL_PATH}/sql/05_dim_stream_ins.sql'},
    )
    load_fact_att = PythonOperator(
        task_id='load_fact_att',
        python_callable=run_sql_file,
        op_kwargs={'sql_path': f'{WSL_PATH}/sql/06_fact_att_ins.sql'},
    )

    trigger_sync = TriggerDagRunOperator(
        task_id='trigger_postgres_sync',
        trigger_dag_id='pupil_datamart_to_postgres',
        conf={
            'prcs_id': '{{ ti.xcom_pull(key="prcs_id", task_ids="start") }}'
        },
    )



    create_tables >> load_csv_to_raw >> load_raw_to_stage >> load_dims_date >> load_dim_pupil >> load_dim_academy >> load_dim_grade >> load_dim_stream >> load_fact_att >> trigger_sync