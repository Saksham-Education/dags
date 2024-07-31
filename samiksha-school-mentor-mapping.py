from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.samiksha_mentor_mapping.mentorMapping import manage_school_mentor_mapping_request

default_args = {
    "owner": "Amit S",
    "depends_on_past": False,
    "start_date": datetime(year=2023, month=5, day=31),
    "email": ["amit@samagragovernance.in"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('samiksha-school-mentor-mapping', default_args=default_args, schedule_interval='0 0 1 * *',
          max_active_runs=1, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

manage_school_mentor_mapping = PythonOperator(
    task_id="manage_school_mentor_mapping",
    dag=dag,
    provide_context=True,
    python_callable=manage_school_mentor_mapping_request)

end = DummyOperator(task_id="end", dag=dag)

start >> manage_school_mentor_mapping >> end
