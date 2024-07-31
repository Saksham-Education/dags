from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.mentor_monthly_report.scrape import create_pdf_request

default_args = {
    "owner": "radhay",
    "depends_on_past": True,
    "start_date": datetime(year=2021, month=12, day=30),
    "email": ["radhay@samagragovernance.in"],

    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('mentor_report_pdf_generate', default_args=default_args, schedule_interval='0 2 1 * *',
          max_active_runs=1, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

create_pdf = PythonOperator(
    task_id="create_pdf",
    dag=dag,
    provide_context=True,
    python_callable=create_pdf_request)

end = DummyOperator(task_id="end", dag=dag)

start >> create_pdf >> end
