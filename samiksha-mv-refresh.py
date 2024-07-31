from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.models import Variable
import psycopg2

mv_list = [
    "coveredschool_aug",
    "primarycoverage_aug",
    "middlecoverage_aug",
    "allcoverage_aug",
    "compliancev1",
    "compliancev2",
    "compliancev3",
    "monthtarget_aug",
    "mentortargetmasterv1",
    "compliancev40",
]


def get_connection():
    uri = Variable.get("samiksha_aggregate")
    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    return cur, conn


def refresh_mv(mv_name, cur, conn):
    cur.execute("""SET work_mem = '2GB'""")
    cur.execute(f'REFRESH MATERIALIZED VIEW {mv_name}')
    conn.commit()


dag = DAG('samiksha-mv-refresh',
          start_date=datetime(2022, 9, 1), schedule_interval="*/180 * * * *", catchup=False)


def refresh_all_mv():
    cur, conn = get_connection()
    for mv in mv_list:
        refresh_mv(mv, cur, conn)
        print("MV has been refreshed for MV", mv)
    conn.close()


start = DummyOperator(task_id="start", dag=dag)

refresh_mv_op = PythonOperator(dag=dag,
                               task_id='refresh_mv',
                               provide_context=False,
                               python_callable=refresh_all_mv,
                               op_args=[],
                               op_kwargs={})

end = DummyOperator(task_id="end", dag=dag)

start >> refresh_mv_op >> end
