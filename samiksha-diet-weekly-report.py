from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.diet.jan23.weekly import create_pdf_request

default_args = {
    "owner": "radhay",
    "depends_on_past": False,
    "start_date": datetime(year=2023, month=1, day=17),
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

dag = DAG('samiksha-diet-weekly-report', default_args=default_args, schedule_interval='0 * * * *',
          max_active_runs=1, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

create_pdf = PythonOperator(
    task_id="create_pdf",
    dag=dag,
    provide_context=True,
    python_callable=create_pdf_request)

end = DummyOperator(task_id="end", dag=dag)

start >> create_pdf >> end
