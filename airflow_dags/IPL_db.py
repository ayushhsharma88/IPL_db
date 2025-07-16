#====================================================================================================================================#
#                                                                                                                                    #
#                 *************************************************************************************************                  #
#                 *           This Is Airflow Dag, used to run and schedule pySpark scripts in sequence           *                  #
#                 *************************************************************************************************                  #
#                                                                                                                                    #
#             Script Name  = IPL_db.py                                                                                               #
#             Description  = This script will run pyspark scripts in order and then mail the status of scripts (success              #
#                            or failed) and then mail logs to provided mail id.                                                      #
#             Arguments    = None                                                                                                    #
#             Dependencies = None                                                                                                    #
#             Author       = Ayush Sharma                                                                                            #
#             Email        = myproject.dea@gmail.com                                                                                 #
#             Date         = 18-04-2025 (dd-mm-yyyy format)                                                                          #
#                                                                                                                                    #
#                                                                                                                                    #
#====================================================================================================================================#


import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.models import DagRun
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
    'email': ['myproject.dea@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False
}

dag = DAG(
    'IPL_db',
    default_args=default_args,
    description='Run PySpark scripts for IPL data in sequence',
    schedule_interval=None,
    catchup=False
)

# --- Query count and send email for specific table ---
def send_table_count_email(**context):
    import os
    from airflow.utils.state import State

    dag_run = context.get("dag_run")
    success_tasks = {
        ti.task_id for ti in dag_run.get_task_instances() if ti.state == State.SUCCESS
    }

    task_to_table = {
        "run_matches": "matches",
        "run_deliveries": "deliveries",
        "run_batsmen": "batsmen",
        "run_bowlers": "bowlers"
    }

    # PostgreSQL connection config
    conn = psycopg2.connect(
        host="192.168.1.9",
        port="5432",
        database="IPL_db",
        user="postgres",
        password="password"
    )

    cur = conn.cursor()

    table_info = ""

    for task_id, table in task_to_table.items():
        if task_id not in success_tasks:
            continue  # Skip failed task tables
        
        try:
            # Query total rows from the table
            cur.execute(f"SELECT COUNT(*) FROM public.{table}")
            total = cur.fetchone()[0]

            # Attempt to read inserted row count from corresponding file
            inserted_file = f"/home/hadoop/row_counts/{table}_count.txt"
            if os.path.exists(inserted_file):
                with open(inserted_file, "r") as f:
                    inserted = int(f.read().strip())
            else:
                inserted = "N/A"

            # Add info to HTML email body
            table_info += f"<b>{table.capitalize()} Table:</b><br>Total Rows: {total}<br>Inserted Rows: {inserted}<br><br>"

        except Exception as e:
            table_info += f"<b>{table.capitalize()} Table:</b> Error fetching data - {str(e)}<br><br>"

    cur.close()
    conn.close()

    # Compose the HTML email content
    html_content = f"""
    <h3>Airflow DAG Succeeded - Row Count Summary</h3>
    {table_info}
    """

    send_email(
        to=["myproject.dea@gmail.com"],
        subject="Row Count Summary - Airflow DAG Success",
        html_content=html_content
    )

# -------- FAILURE EMAIL WITH LOG ATTACHMENTS --------
def send_failure_email(**context):
    dag_run = context.get("dag_run")
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['execution_date']
    timestamp = execution_date.strftime('%Y-%m-%dT%H:%M:%S')  # Match Airflow timestamp format

    failed_tasks = [ti for ti in dag_run.get_task_instances() if ti.state == State.FAILED and ti.task_id != "generate_report_email"]
    if not failed_tasks:
        return

    attached_logs = []
    for ti in failed_tasks:
        try_number = ti.try_number if ti.try_number > 0 else 1
        execution_ts = execution_date.isoformat()
        log_path = f"/home/hadoop/airflow/cust_logs/{dag_id}_{ti.task_id}_{execution_ts}.log"

        if os.path.exists(log_path):
            attached_logs.append(log_path)
        else:
            tmp_log = f"/tmp/{ti.task_id}_log_missing.txt"
            with open(tmp_log, 'w') as f:
                f.write(f"Log file not found for {ti.task_id} at expected path: {log_path}")
            attached_logs.append(tmp_log)


    html_content = f"""
    <h3>Airflow DAG Failed</h3>
    <p><b>DAG:</b> {dag_id}<br>
    <b>Run ID:</b> {run_id}<br>
    <b>Execution Date:</b> {execution_date}</p>
    <p>Failed task logs are attached to this email.</p>
    """

    send_email(
        to=["myproject.dea@gmail.com"],
        subject=f"[Failure] Airflow DAG: {dag_id}",
        html_content=html_content,
        files=attached_logs
    )


# -------- SUCCESS EMAIL --------
def send_success_email(**context):
    dag_run = context.get("dag_run")
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['execution_date']
    timestamp = execution_date.strftime('%Y-%m-%dT%H:%M:%S')

    attached_logs = []
    for ti in dag_run.get_task_instances():
        if ti.state == State.SUCCESS and ti.task_id not in ["generate_report_email", "send_failure_email", "send_success_email"]:
            try_number = ti.try_number if ti.try_number > 0 else 1
            execution_ts = execution_date.isoformat()
            log_path = f"/home/hadoop/airflow/cust_logs/{dag_id}_{ti.task_id}_{execution_ts}.log"
            if os.path.exists(log_path):
                attached_logs.append(log_path)

    html_content = f"""
    <h3>Airflow DAG Succeeded</h3>
    <p><b>DAG:</b> {dag_id}<br>
    <b>Run ID:</b> {run_id}<br>
    <b>Execution Date:</b> {execution_date}</p>
    <p>Success tasks logs are attached to this email.</p>
    """

    send_email(
        to=["myproject.dea@gmail.com"],
        subject=f"[Success] Airflow DAG: {dag_id}",
        html_content=html_content,
        files=attached_logs
    )

# -------- TASKS --------
matches_task = BashOperator(
    task_id='run_matches',
    bash_command='/home/hadoop/spark/bin/spark-submit /home/hadoop/scripts/matches.py',
    dag=dag
)

deliveries_task = BashOperator(
    task_id='run_deliveries',
    bash_command='/home/hadoop/spark/bin/spark-submit /home/hadoop/scripts/deliveries.py',
    dag=dag
)

batsmen_task = BashOperator(
    task_id='run_batsmen',
    bash_command='/home/hadoop/spark/bin/spark-submit /home/hadoop/scripts/batsmen.py',
    dag=dag
)

bowlers_task = BashOperator(
    task_id='run_bowlers',
    bash_command='/home/hadoop/spark/bin/spark-submit /home/hadoop/scripts/bowlers.py',
    dag=dag
)

generate_report_task = PythonOperator(
    task_id='generate_report_email',
    python_callable=send_table_count_email,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)


# -------- FINAL SUCCESS EMAIL TASK --------
final_success_email = PythonOperator(
    task_id='send_success_email',
    python_callable=send_success_email,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,  # Only when any previous task succeed
    dag=dag
)

# -------- FINAL FAILURE EMAIL TASK --------
final_failure_email = PythonOperator(
    task_id='send_failure_email',
    python_callable=send_failure_email,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_FAILED,  # Only when any previous task fails
    dag=dag
)

# -------- DEPENDENCIES --------
matches_task >> deliveries_task
[matches_task, deliveries_task, batsmen_task, bowlers_task] >> final_success_email
[matches_task, deliveries_task, batsmen_task, bowlers_task] >> generate_report_task
[matches_task, deliveries_task, batsmen_task, bowlers_task] >> final_failure_email

