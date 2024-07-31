import requests
import json
import psycopg2
import logging
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from psycopg2.extras import RealDictCursor
from airflow.models import Variable

# from .template_dict_store import Dict


logger = logging.getLogger()
logger.setLevel(logging.INFO)
now = datetime.now()

#################################################
#               Configs                         #
#################################################
__db_uri__ = Variable.get("mentor-pdf-data-db")
__pdf_service_url__ = Variable.get("pdf-make-service_url")
__mentor_report_hasura_api__ = Variable.get("mentor-report-hasura-api")
__mentor_report_hasura_secret__ = Variable.get("mentor-report-hasura-secret")
__table_name__ = 'ELM_MY22_CORE'
__col_name__ = 'kys_report_pdf_status'
headers = {
  'x-hasura-admin-secret': __mentor_report_hasura_secret__,
  'Content-Type': 'application/json'
}
payload = """mutation insert_know_your_school_report{
      insert_know_your_school_report(
        objects: [
          {
            academic_year : "%academic_year",
            school_id : "%school_id",
            pdf_url : "%url"
          }
        ]
        on_conflict: {
          constraint: know_your_school_report_pkey,
          update_columns: [pdf_url]
        }
      ) {
        affected_rows
        returning {
          academic_year,
          school_id,
          pdf_url
        }
      }
    }
    """


def get_connection():
    uri = __db_uri__
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def call_pdf_api(id, month):
    query_param = {
      'id': id,
      'month': month
    }
    url = __pdf_service_url__ + 'knowYourSchool'
    r = requests.get(url, params=query_param)
    return r.status_code, r.json()


def update_record_pdf_url(pdf_url, school, year):
    body = payload.replace("%academic_year", str(int(year))).replace("%school_id", school).replace("%url", pdf_url)
    response = requests.request("POST", __mentor_report_hasura_api__, headers=headers, json={"query": body})
    if response.status_code == 200:
        return "success"
    else:
        return "failed"


def update_record_report_flag(_uri, cur):
    query = """UPDATE "{}" SET {}='{}' where "_URI"='{}'""".format(__table_name__, __col_name__, 'pdf generated', _uri)
    cur.execute(query)


def create_pdf_request(**context):
    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(days=1)
    query = """
      SELECT
        "_CREATION_DATE",
        "_MARKED_AS_COMPLETE_DATE",
        EXTRACT(MONTH FROM "_CREATION_DATE") AS "MONTH",
        EXTRACT(YEAR FROM "_CREATION_DATE") AS "YEAR",
        "OFFICER_DETAILS_SCHOOL",
        "_URI"
      FROM "{}" 
      where EXTRACT(DAY FROM "_MARKED_AS_COMPLETE_DATE") >= {} and EXTRACT(MONTH FROM "_MARKED_AS_COMPLETE_DATE") = {} and EXTRACT(YEAR FROM "_MARKED_AS_COMPLETE_DATE") = {} and "{}" IS NULL
      """.format(__table_name__, report_date.day, report_date.month, report_date.year, __col_name__)
    cur.execute(query)
    records = cur.fetchall()
    conn.commit()
    conn.close()
    logging.info(f"Total no of records present in {__table_name__} table on {dt_string}: {len(records)}")
    for record in records:
#         time.sleep(2)
        try:
            cur, conn = get_connection()
        except psycopg2.InterfaceError:
            cur, conn = get_connection()
        logging.info(f"For _URI : {record['_URI']} and School : {record['OFFICER_DETAILS_SCHOOL']}")
        status_code, response = call_pdf_api(record['OFFICER_DETAILS_SCHOOL'], record['MONTH'])
        if status_code == 200 and response['status'] != 0:
            logging.info(f"{dt_string}: PDF generated successfully for {record['_URI']}: Response=> {response['status']}")
            resp = update_record_pdf_url(response['pdfs'][0]['url'], record['OFFICER_DETAILS_SCHOOL'], record['YEAR'])
            if resp == "success":
                update_record_report_flag(record['_URI'], cur)
                logging.info(f"{dt_string}: PDF status updated for {record['_URI']}")
        else:
            logging.info(
                f"{dt_string}: Failed to generate PDF for _URI: {record['_URI']} and School: {record['OFFICER_DETAILS_SCHOOL']}, Response=> {response}")
    conn.commit()
    conn.close()
