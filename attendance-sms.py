from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.attendance_sms.send_sms import handle_absent_sms
from deps.attendance_sms.send_sms import handle_present_sms

default_args = {
    "owner": "radhay",
    "depends_on_past": False,
    "start_date": datetime(year=2022, month=8, day=22),
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

dag = DAG('attendance-sms', default_args=default_args, schedule_interval='0 13 * * *',
          max_active_runs=1, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

absent_sms_sent = PythonOperator(
    task_id="absent_sms_sent",
    dag=dag,
    provide_context=True,
    python_callable=handle_absent_sms)

present_sms_sent = PythonOperator(
    task_id="present_sms_sent",
    dag=dag,
    provide_context=True,
    python_callable=handle_present_sms)


end = DummyOperator(task_id="end", dag=dag)

start >> [absent_sms_sent, present_sms_sent] >> end
