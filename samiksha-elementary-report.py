from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.mentoring_forms.june23.elementary import create_pdf_request as create_pdf_june23_request
from deps.mentoring_forms.oct23.elementary import create_pdf_request as create_pdf_oct23_request
from deps.mentoring_forms.oct23.elementary_staging import create_pdf_request as create_pdf_oct23_request_staging
from deps.mentoring_forms.oct23.elementary2 import create_pdf_request as create_pdf_sept23_request

default_args = {
    "owner": "radhay",
    "depends_on_past": False,
    "start_date": datetime(year=2022, month=3, day=11),
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

dag = DAG('samiksha-elementary-report', default_args=default_args, schedule_interval='30,30 9,15 * * *',
          max_active_runs=1, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

create_june23_pdf = PythonOperator(
    task_id="create_june23_pdf",
    dag=dag,
    provide_context=True,
    python_callable=create_pdf_june23_request)

create_sept23_pdf = PythonOperator(
    task_id="create_sept23_pdf",
    dag=dag,
    provide_context=True,
    python_callable=create_pdf_sept23_request)

create_oct23_pdf = PythonOperator(
    task_id="create_oct23_pdf",
    dag=dag,
    provide_context=True,
    python_callable=create_pdf_oct23_request)

create_oct23_pdf_staging = PythonOperator(
    task_id="create_oct23_pdf_staging",
    dag=dag,
    provide_context=True,
    python_callable=create_pdf_oct23_request_staging)

end = DummyOperator(task_id="end", dag=dag)

start >> [create_june23_pdf, create_sept23_pdf, create_oct23_pdf, create_oct23_pdf_staging] >> end
