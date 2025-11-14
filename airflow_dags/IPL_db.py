import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.email import send_email
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.models import DagRun
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import os
import subprocess
from airflow.exceptions import AirflowSkipException
from airflow.dags.utils.spark_submit import conditional_spark_submit

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

# --- Define branch logic ---
# -------- BRANCH FUNCTIONS FOR ALL TABLES --------

def branch_on_batsmen(**kwargs):
    ti = kwargs['ti']
    file_exists = ti.xcom_pull(key='file_exists', task_ids='check_batsmen')
    if file_exists:
        return 'batsmen_task'  # Run Spark
    else:
        return 'skip_batsmen'  # Skip path


def branch_on_matches(**kwargs):
    ti = kwargs['ti']
    file_exists = ti.xcom_pull(key='file_exists', task_ids='check_matches')
    if file_exists:
        return 'matches_task'
    else:
        return 'skip_matches'


def branch_on_deliveries(**kwargs):
    ti = kwargs['ti']
    file_exists = ti.xcom_pull(key='file_exists', task_ids='check_deliveries')
    if file_exists:
        return 'deliveries_task'
    else:
        return 'skip_deliveries'


def branch_on_bowlers(**kwargs):
    ti = kwargs['ti']
    file_exists = ti.xcom_pull(key='file_exists', task_ids='check_bowlers')
    if file_exists:
        return 'bowlers_task'
    else:
        return 'skip_bowlers'


# -------- FILE CHECK FUNCTION --------
def check_file_exists(path, **kwargs):
    import subprocess
    import fnmatch
    import logging

    dir_path, pattern = path.rsplit('/', 1)
    logging.info(f">>>> Running: hdfs dfs -ls {dir_path}")
    result = subprocess.run(['hdfs', 'dfs', '-ls', dir_path], capture_output=True, text=True)
    output = result.stdout.strip()

    logging.info(f">>>> HDFS output:\n{output}")

    lines = output.split('\n')
    matched_files = []

    for line in lines:
        parts = line.split()
        if len(parts) < 8:
            continue
        filename = parts[-1]
        if fnmatch.fnmatch(filename, f"{dir_path}/{pattern}"):
            matched_files.append(filename)

    file_exists = bool(matched_files)

    if not file_exists:
        logging.info(f"No files matched for pattern {path}")
    else:
        logging.info(f"Files found: {matched_files}")

    # Push to XCom explicitly
    kwargs['ti'].xcom_push(key='file_exists', value=file_exists)

    return file_exists


# --- Query count and send email for specific table ---
def send_table_count_email(**context):
    import os
    from airflow.utils.state import State

    critical_tasks = {"matches_task", "deliveries_task", "batsmen_task", "bowlers_task"}

    dag_run = context.get("dag_run")
    success_tasks = {
        ti.task_id for ti in dag_run.get_task_instances() if ti.state == State.SUCCESS
    }

    # Check if all critical tasks succeeded
    if not (success_tasks & critical_tasks):
        # Skip this task if any critical task failed
        raise AirflowSkipException("Skipping email because one or more critical tasks failed.")

    task_to_table = {
        "matches_task": "matches",
        "deliveries_task": "deliveries",
        "batsmen_task": "batsmen",
        "bowlers_task": "bowlers"
    }

    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="IPL_db",
        user="postgres",
        password="password"
    )


    cur = conn.cursor()
    rows_html = ""

    for task_id, table in task_to_table.items():
        if task_id not in success_tasks:
            continue  # Skip failed/skipped task tables

        try:
            cur.execute(f"SELECT COUNT(*) FROM public.{table}")
            total = cur.fetchone()[0]

            inserted_file = f"/home/hadoop/row_counts/{table}_count.txt"
            if os.path.exists(inserted_file):
                with open(inserted_file, "r") as f:
                    inserted = int(f.read().strip())
            else:
                inserted = "N/A"

            rows_html += f"<tr><td>{table.capitalize()}</td><td>{total}</td><td>{inserted}</td></tr>"
        except Exception as e:
            rows_html += f"<tr><td>{table.capitalize()}</td><td colspan='2'>Error: {str(e)}</td></tr>"

    cur.close()
    conn.close()

    html_content = f"""
    <h3>Loading Successful - Row Count Summary</h3>
    <table border ="1" cellpadding="5" cellspacing="0">
        <tr>
            <th>Table</th>
            <th>Total Rows</th>
            <th>Inserted Rows</th>
        </tr>
        {rows_html}
    </table>
    """

    send_email(
        to=["myproject.dea@gmail.com"],
        subject="Row Count Summary",
        html_content=html_content
    )

# -------- FAILURE EMAIL WITH LOG ATTACHMENTS --------
def send_failure_email(**context):
    dag_run = context.get("dag_run")
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['execution_date']
    timestamp = execution_date.strftime('%Y-%m-%dT%H:%M:%S')

    # 1. Check if all critical tasks succeeded
    critical_tasks = {"matches_task", "deliveries_task", "batsmen_task", "bowlers_task"}

    failed_tasks = {
        ti.task_id for ti in dag_run.get_task_instances() if ti.state == State.FAILED
    }

    if not critical_tasks & failed_tasks:
        raise AirflowSkipException("No critical tasks failed — skipping failure email.")

    attached_logs = []
    for ti in dag_run.get_task_instances():
        if ti.state == State.FAILED and not ti.task_id.startswith(("check_", "branch_on_")) \
            and ti.task_id not in ["generate_report_email", "send_failure_email", "send_success_email"]:
            try_number = ti.try_number if ti.try_number > 0 else 1
            execution_ts = execution_date.isoformat()
            log_path = f"/home/hadoop/airflow/cust_logs/{dag_id}_{ti.task_id}_{execution_ts}.log"
            if os.path.exists(log_path):
                attached_logs.append(log_path)


    html_content = f"""
    <h3>Airflow DAG Failed</h3>
    <p><b>DAG:</b> {dag_id}<br>
    <b>Run ID:</b> {run_id}<br>
    <b>Execution Date:</b> {execution_date}</p>
    <p>Failed task logs are attached to this email.</p>
    """

    send_email(
        to=["myproject.dea@gmail.com"],
        subject=f"Failure In Pipeline: {dag_id}",
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

    # 1. Check if all critical tasks succeeded
    critical_tasks = {"matches_task", "deliveries_task", "batsmen_task", "bowlers_task"}

    success_tasks = {
        ti.task_id for ti in dag_run.get_task_instances() if ti.state == State.SUCCESS
    }

    if not critical_tasks & success_tasks:
        raise AirflowSkipException("No critical tasks succeeded — skipping success email.")

    attached_logs = []
    for ti in dag_run.get_task_instances():
        if ti.state == State.SUCCESS and not ti.task_id.startswith(("check_", "branch_on_")) \
            and ti.task_id not in ["generate_report_email", "send_failure_email", "send_success_email"]:
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
        subject=f"Success In Pipeline: {dag_id}",
        html_content=html_content,
        files=attached_logs
    )

# -------- TASKS --------

# Matches
check_matches = PythonOperator(
    task_id='check_matches',
    python_callable=check_file_exists,
    op_kwargs={'path': 'hdfs://localhost:9000/files/matches/matches_*.csv'},
    provide_context=True,
    dag=dag,
)

branch_task_matches = BranchPythonOperator(
    task_id='branch_on_matches',
    python_callable=branch_on_matches,
    provide_context=True,
    dag=dag,
)

matches_task = BashOperator(
    task_id='matches_task',
    bash_command='spark-submit /home/hadoop/scripts/matches.py',
    dag=dag,
)

skip_matches = DummyOperator(
    task_id='skip_matches',
    dag=dag,
)

# --- DAG sequence ---
check_matches >> branch_task_matches >> [matches_task, skip_matches]

# Deliveries
check_deliveries = PythonOperator(
    task_id='check_deliveries',
    python_callable=check_file_exists,
    op_kwargs={'path': 'hdfs://localhost:9000/files/deliveries/deliveries_*.csv'},
    provide_context=True,
    dag=dag,
)

branch_task_deliveries = BranchPythonOperator(
    task_id='branch_on_deliveries',
    python_callable=branch_on_deliveries,
    provide_context=True,
    dag=dag,
)

deliveries_task = BashOperator(
    task_id='deliveries_task',
    bash_command='spark-submit /home/hadoop/scripts/deliveries.py',
    dag=dag,
)

skip_deliveries = DummyOperator(
    task_id='skip_deliveries',
    dag=dag,
)

# --- DAG sequence ---
check_deliveries >> branch_task_deliveries >> [deliveries_task, skip_deliveries]

# Batsmen
check_batsmen = PythonOperator(
    task_id='check_batsmen',
    python_callable=check_file_exists,
    op_kwargs={'path': 'hdfs://localhost:9000/files/batsmen/batsmen_*.csv'},
    provide_context=True,
    dag=dag,
)

branch_task_batsmen = BranchPythonOperator(
    task_id='branch_on_batsmen',
    python_callable=branch_on_batsmen,
    provide_context=True,
    dag=dag,
)

batsmen_task = BashOperator(
    task_id='batsmen_task',
    bash_command='spark-submit /home/hadoop/scripts/batsmen.py',
    dag=dag,
)

skip_batsmen = DummyOperator(
    task_id='skip_batsmen',
    dag=dag,
)

# --- DAG sequence ---
check_batsmen >> branch_task_batsmen >> [batsmen_task, skip_batsmen]

# Bowlers
check_bowlers = PythonOperator(
    task_id='check_bowlers',
    python_callable=check_file_exists,
    op_kwargs={'path': 'hdfs://localhost:9000/files/bowlers/bowlers_*.csv'},
    provide_context=True,
    dag=dag,
)

branch_task_bowlers = BranchPythonOperator(
    task_id='branch_on_bowlers',
    python_callable=branch_on_bowlers,
    provide_context=True,
    dag=dag,
)

bowlers_task = BashOperator(
    task_id='bowlers_task',
    bash_command='spark-submit /home/hadoop/scripts/bowlers.py',
    dag=dag,
)

skip_bowlers = DummyOperator(
    task_id='skip_bowlers',
    dag=dag,
)

# --- DAG sequence ---
check_bowlers >> branch_task_bowlers >> [bowlers_task, skip_bowlers]

# -------- REPORT & EMAIL TASKS --------
# --- Join point: wait for all 4 run_* tasks ---
join_runs = EmptyOperator(
    task_id='join_runs',
    trigger_rule=TriggerRule.ALL_DONE,  # wait for all 4 run tasks (success/failed/skipped)
    dag=dag
)

# --- Success email ---
final_success_email = PythonOperator(
    task_id='send_success_email',
    python_callable=send_success_email,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,  # at least one succeeded
    dag=dag
)

# --- Failure email ---
final_failure_email = PythonOperator(
    task_id='send_failure_email',
    python_callable=send_failure_email,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,  # trigger if at least one failed
    dag=dag
)

# --- Report email ---
generate_report_task = PythonOperator(
    task_id='generate_report_email',
    python_callable=send_table_count_email,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,  # always run (success/failed/skipped tasks)
    dag=dag
)


# -------- DEPENDENCIES --------
matches_task >> deliveries_task
[matches_task, deliveries_task, batsmen_task, bowlers_task] >> join_runs >> final_success_email
[matches_task, deliveries_task, batsmen_task, bowlers_task] >> join_runs >> generate_report_task
[matches_task, deliveries_task, batsmen_task, bowlers_task] >> join_runs >> final_failure_email
