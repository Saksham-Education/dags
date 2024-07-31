from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.samarth_mvs.student_content_share_scrape import send_dummy_sms_task, scrape

default_args = {
    "owner": "radhay",
    "depends_on_past": True,
    "start_date": datetime(year=2021, month=12, day=25),
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

dag = DAG('hgp_student_content_share_sms', default_args=default_args, schedule_interval='@hourly',
          max_active_runs=1, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

# send_dummy_sms = PythonOperator(
#     task_id="send_dummy_sms",
#     dag=dag,
#     provide_context=True,
#     python_callable=send_dummy_sms_task)

send_sms_from_view = PythonOperator(dag=dag,
                                    task_id='send_sms_from_view',
                                    provide_context=True,
                                    python_callable=scrape,
                                    )

end = DummyOperator(task_id="end", dag=dag)

start >> send_sms_from_view >> end
