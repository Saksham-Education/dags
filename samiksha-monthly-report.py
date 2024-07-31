from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.mentoring_forms.june23.monthly_report import create_pdf_request as create_pdf_june23_request
from deps.mentoring_forms.oct23.monthly_report import create_pdf_request as create_pdf_oct23_request
from deps.mentoring_forms.oct23.monthly_report_staging import create_pdf_request as create_pdf_oct23_request_staging

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

dag = DAG('samiksha-monthly-report', default_args=default_args, schedule_interval='0 18 2 * *',
          max_active_runs=1, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

create_pdf_june23 = PythonOperator(
    task_id="create_pdf_june23",
    dag=dag,
    provide_context=True,
    python_callable=create_pdf_june23_request)

create_pdf_oct23 = PythonOperator(
    task_id="create_pdf_oct23",
    dag=dag,
    provide_context=True,
    python_callable=create_pdf_oct23_request)

create_pdf_oct23_staging = PythonOperator(
    task_id="create_pdf_oct23_staging",
    dag=dag,
    provide_context=True,
    python_callable=create_pdf_oct23_request_staging)

end = DummyOperator(task_id="end", dag=dag)

start >> [create_pdf_june23, create_pdf_oct23, create_pdf_oct23_staging] >> end
