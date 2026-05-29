from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import subprocess
import socket
import duckdb
import time
import json
import os
import sys

# ─────────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────────
default_args = {
    'owner': 'retailx',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email_on_failure': False,
    'email_on_retry': False,
}


# ─────────────────────────────────────────────
# TASKS
# ─────────────────────────────────────────────
def check_kafka_health():
    """Cek apakah Kafka broker bisa direach"""
    kafka_host = Variable.get("kafka_host", default_var="kafka")
    kafka_port = int(Variable.get("kafka_port", default_var="29092"))

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((kafka_host, kafka_port))
        sock.close()

        if result == 0:
            print(f"Kafka broker {kafka_host}:{kafka_port} is healthy!")
            return True
        else:
            raise Exception(f"Kafka broker {kafka_host}:{kafka_port} is not reachable!")
    except Exception as e:
        raise Exception(f"Kafka health check failed: {e}")


def truncate_raw_tables(**context):
    force_truncate = Variable.get("force_truncate", default_var="false").lower() == "true"
    
    if not force_truncate:
        print("Skipping truncate — force_truncate=false. Data lama dipertahankan.")
        return

    conn = duckdb.connect(Variable.get("duckdb_path", default_var="/opt/data/retailx.duckdb"))
    tables = ["raw.orders", "raw.order_items", "raw.payments"]
    print("force_truncate=true, mulai truncate...")
    for table in tables:
        conn.execute(f"TRUNCATE {table}")
        print(f"  [truncate] {table} ✓")
    conn.close()
    print("Semua raw tables berhasil di-truncate.")


def run_consumer_task():
    """Jalankan consumer untuk batch ingestion"""
    kafka_host = Variable.get("kafka_host", default_var="kafka")
    kafka_port = Variable.get("kafka_port", default_var="29092")
    bootstrap  = f"{kafka_host}:{kafka_port}"
    db_path    = Variable.get("duckdb_path", default_var="/opt/data/retailx.duckdb")

    print(f"Running consumer batch — bootstrap: {bootstrap}, db: {db_path}")

    result = subprocess.run(
        ["python", "/opt/consumer/kafka_consumer.py"],
        capture_output=True,
        text=True,
        timeout=3600,
        env={**os.environ,
             "KAFKA_BOOTSTRAP": bootstrap,
             "DB_PATH": db_path}
    )

    print("STDOUT:", result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        raise Exception(f"Consumer failed with return code {result.returncode}")

    print("Consumer batch selesai.")


def notify_success():
    print("=" * 50)
    print("SUCCESS: RetailX ETL Pipeline selesai!")
    print(f"Waktu: {datetime.utcnow().isoformat()}")
    print("=" * 50)


# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────
with DAG(
    dag_id='daily_retail_etl',
    default_args=default_args,
    description='RetailX daily ETL pipeline',
    # schedule_interval=None, buat debug
    schedule_interval='0 2 * * *',  # jam 2 pagi UTC setiap hari
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['retailx', 'etl', 'kafka', 'dbt'],
) as dag:

    task_check_kafka = PythonOperator(
        task_id='check_kafka_health',
        python_callable=check_kafka_health,
    )

    task_truncate = PythonOperator(
        task_id='truncate_raw_tables',
        python_callable=truncate_raw_tables,
    )

    task_run_consumer = PythonOperator(
        task_id='run_consumer',
        python_callable=run_consumer_task,
    )

    task_dbt_staging = BashOperator(
    task_id='run_dbt_staging',
    bash_command='cd /opt/dbt/retailx_dbt && dbt run --select staging --profiles-dir /opt/dbt/retailx_dbt',
    )

    task_dbt_intermediate = BashOperator(
        task_id='run_dbt_intermediate',
        bash_command='cd /opt/dbt/retailx_dbt && dbt run --select int_order_items_agg int_payments_agg --profiles-dir /opt/dbt/retailx_dbt',
    )

    task_dbt_marts = BashOperator(
        task_id='run_dbt_marts',
        bash_command='cd /opt/dbt/retailx_dbt && dbt run --select marts --profiles-dir /opt/dbt/retailx_dbt',
    )
    task_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/dbt/retailx_dbt && dbt test --profiles-dir /opt/dbt/retailx_dbt',
    )

    task_notify = PythonOperator(
        task_id='notify_success',
        python_callable=notify_success,
    )

    # Task dependencies
    task_check_kafka >> task_truncate >> task_run_consumer >> task_dbt_staging >> task_dbt_intermediate >> task_dbt_marts >> task_dbt_tests >> task_notify