from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='partition_deletion',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # every 5 minutes
    catchup=False,
    max_active_runs=1,  # prevent overlapping DAG runs
    description='Runs monitoring_script.py every 5 minutes',
    tags=['monitoring', 'hdfs', 'postgres'],
) as dag:

    run_monitoring_script = BashOperator(
        task_id='run_monitoring_script',
        bash_command='python3 /home/hadoop/scripts/partition_deletion.py'
    )

    run_monitoring_script

