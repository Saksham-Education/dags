from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import AirflowException
from airflow.exceptions import AirflowFailException

import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine
from deps.logParser import utility_mpc as util

logger = logging.getLogger()
logger.setLevel(logging.INFO)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 22, 0, 0, 0),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def db_connect(conn_string):
    """ Connect to the database server
    Args:
        conn_string: database connection string
    Returns:
        connection object
    """
    conn = None
    try:
        logging.info('Connecting to the database ...')
        if conn_string != None:
            engine = create_engine(conn_string)
            conn = engine.connect()
        else:
            logging.error("No database connection string set in environment.")
    except Exception as e:
        logging.error(e)
        raise AirflowException("Can't connect to database")
    return conn

def bulk_db_upload(dataframe, table_name, conn):
    logging.info('insert into database started')
    dataframe.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
    logging.info('insert into database completed')

def log_parser(**kwargs):
    DATABASE_URL = Variable.get("log_parser_mpc")
    parsing_status, df_log_dashboard_events, df_dashboard_event_filter = util.log_parser()
    if not parsing_status:
        raise AirflowFailException("Error in processing log file")

    conn = db_connect(DATABASE_URL)
    try:
        bulk_db_upload(df_log_dashboard_events, 'log_dashboard_events', conn)
        bulk_db_upload(df_dashboard_event_filter, 'dashboard_event_filter', conn)
    except Exception as e:
        logging.error(e)
        raise AirflowException("Error in DB Push")
    
dag = DAG('log_parser_mpc', default_args=default_args, schedule_interval='0 2 * * *', max_active_runs=1)

start = DummyOperator(task_id="start", dag=dag)
parse_log = PythonOperator(
    dag=dag,
    task_id='log_parser_mpc_task',
    provide_context=True,
    python_callable=log_parser)
end = DummyOperator(task_id="end", dag=dag)

start >> parse_log >> end
