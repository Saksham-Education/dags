from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.samarth.cdac_send_sms import send_sms

default_args = {
    "owner": "radhay",
    "depends_on_past": True,
    "start_date": datetime(year=2022, month=1, day=31),
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

dag = DAG('hgp_cdac_sms_task', default_args=default_args, max_active_runs=1, schedule_interval=None, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

send_sms = PythonOperator(
    task_id="create_pdf",
    dag=dag,
    provide_context=True,
    python_callable=send_sms)

end = DummyOperator(task_id="end", dag=dag)

start >> send_sms >> end
