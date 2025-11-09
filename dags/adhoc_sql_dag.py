"""
Adhoc SQL runner
Use this dag to run any SQL file against the pupil datamart DB.

Pass the SQL file path in the DAG run config:
You can use the normal windows path:
{
  "sql_file": "C:/Users/PC/Documents/newglobe-pupil-datamart/sql/06_fact_att_ins.sql"
}
or (WSL path)
{
  "sql_file": "/mnt/c/Users/PC/Documents/newglobe-pupil-datamart/sql/count_checker.sql"
}
"""



from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3
import sys
from pathlib import Path

# -------------------------------------------------
# ONLY CHANGE THIS IF YOUR DB PATH MOVES
# -------------------------------------------------
BASE_PATH = r"C:/Users/PC/Documents/newglobe-pupil-datamart"
WSL_ROOT  = BASE_PATH.replace("C:", "/mnt/c").replace("\\", "/")
DB_PATH   = f"{WSL_ROOT}/newglobe_pupil_datamart.db"
# -------------------------------------------------

def execute_sql(**context):
    sql_file = context["dag_run"].conf.get("sql_file")
    if not sql_file:
        raise ValueError("Missing 'sql_file' in conf")

    sql_path = Path(sql_file.replace("C:", "/mnt/c").replace("\\", "/"))
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    print(f"Executing: {sql_path}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        sql_content = sql_path.read_text(encoding="utf-8").strip()
        if sql_content.endswith(';'):
            sql_content = sql_content[:-1]

        statements = [s.strip() for s in sql_content.split(';') if s.strip()]

        if len(statements) > 1:
            print(f"Running {len(statements)} statements with executescript()...")
            cursor.executescript(sql_content)
            conn.commit()
            print("All statements executed.")
        else:
            stmt = statements[0]
            cursor.execute(stmt)

            if cursor.description:                    
                cols = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                col_widths = []
                for i, col in enumerate(cols):
                    w = max(len(col), max((len(str(row[i])) for row in rows), default=0))
                    col_widths.append(w)

                header = " | ".join(f"{c:<{w}}" for c, w in zip(cols, col_widths))
                print(header)
                print("-" * len(header))

                for row in rows:
                    print(" | ".join(f"{str(cell):<{w}}" for cell, w in zip(row, col_widths)))

                print(f"\n{len(rows)} row(s) returned.\n")
            else:
                conn.commit()
                print(f"{cursor.rowcount} row(s) affected.")

    except Exception as e:
        conn.rollback()
        print(f"SQL ERROR: {e}")
        raise
    finally:
        conn.close()

    print("\nExecution completed.")

with DAG(
    dag_id="run_sql_adhoc",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["utility"],
) as dag:
    run = PythonOperator(
        task_id="run_sql",
        python_callable=execute_sql,
        provide_context=True,
    )