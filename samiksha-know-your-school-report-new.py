from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.mentoring_forms.kys import create_pdf_request
from deps.mentoring_forms.kys_staging import create_pdf_request as create_pdf_staging

default_args = {
    "owner": "radhay",
    "depends_on_past": False,
    "start_date": datetime(year=2023, month=2, day=8),
    "email": ["kanav@samagragovernance.in"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('samiksha-know-your-school-report-new', default_args=default_args, schedule_interval='30 8 * * *',
          max_active_runs=1, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

create_pdf = PythonOperator(
    task_id="create_pdf",
    dag=dag,
    provide_context=True,
    python_callable=create_pdf_request)

create_kys_pdf_staging = PythonOperator(
    task_id="create_pdf_staging",
    dag=dag,
    provide_context=True,
    python_callable=create_pdf_staging)

end = DummyOperator(task_id="end", dag=dag)

start >> [create_pdf, create_kys_pdf_staging] >> end
