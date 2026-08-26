from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Base path where the project is mounted inside the container
PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "instramart",
    "retries": 0,
}

with DAG(
    dag_id="instramart_pipeline",
    default_args=default_args,
    description="Bronze validation -> Silver cleaning -> Gold metrics",
    schedule=None,          # manual trigger only, for now
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["instramart"],
) as dag:

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        bash_command=f"cd {PROJECT_DIR} && python check_data.py",
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=f"cd {PROJECT_DIR} && python silver_layer.py",
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=f"cd {PROJECT_DIR} && python gold_layer.py",
    )

    validate_bronze >> build_silver >> build_gold