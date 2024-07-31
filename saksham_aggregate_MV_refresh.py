from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import psycopg2

mv_list = ["ement_master2","ementor1","fementor","evisitlog1","evisitlog2","allevisitlogs","evisitcompl","insights1","insights2","allinsights"]


def get_connection():
    uri = Variable.get("samiksha_aggregate")
    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    return cur, conn


def refresh_mv(**kwargs):
    cur, conn = get_connection()
    cur.execute("""SET work_mem = '2GB'""")
    cur.execute('REFRESH MATERIALIZED VIEW {}'.format(kwargs["mv_name"]))
    conn.commit()
    conn.close()


with DAG('saksham_MV_refresh', start_date=datetime(2021, 6, 22), schedule_interval="@hourly") as dag:

    def get_mv(**kwargs):
        print(kwargs)
        return mv_list

    def group(mv_name, **kwargs):
        # load the values if needed in the command you plan to execute
        return PythonOperator(
            task_id='MaterializedView--{}'.format(mv_name),
            python_callable=refresh_mv,
            op_kwargs={'mv_name': mv_name},
            dag=dag)

    push_func = PythonOperator(
        task_id='push_func',
        provide_context=True,
        python_callable=get_mv,
        dag=dag)

    complete = DummyOperator(
        task_id='All_Done',
        dag=dag)

    for i in get_mv():
        push_func >> group(i) >> complete
