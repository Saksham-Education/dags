from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import psycopg2

mv_list = ["student_attendance_vff","std_count_vff","compliance_school_test","compliance_school_v5","compliance_count","teacher_attendance_total","teacher_attendance_vff","teacher_count_vff","compliance_schoolhead_list","compliance_schoolhead_count"]


def get_connection():
    uri = Variable.get("shc_student_attendance")
    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    return cur, conn


def refresh_mv(**kwargs):
    cur, conn = get_connection()
    cur.execute("""SET work_mem = '6GB'""")
    cur.execute('REFRESH MATERIALIZED VIEW {}'.format(kwargs["mv_name"]))
    conn.close()


with DAG('MPC_Technosys_RMV_Attendance', start_date=datetime(2021, 4, 1), schedule_interval= "1 */6 * * *", catchup=False) as dag:

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
