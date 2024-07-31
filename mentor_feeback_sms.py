from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.mentor_feedback_sms.send_sms import handle_sms_send

default_args = {
    "owner": "radhay",
    "depends_on_past": False,
    "start_date": datetime(year=2022, month=9, day=15),
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

dag = DAG('mentor-feedback-sms', default_args=default_args, schedule_interval='30 14 * * *',
          max_active_runs=1, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

send_sms = PythonOperator(
    task_id="send_sms",
    dag=dag,
    provide_context=True,
    python_callable=handle_sms_send)

end = DummyOperator(task_id="end", dag=dag)

start >> send_sms >> end
