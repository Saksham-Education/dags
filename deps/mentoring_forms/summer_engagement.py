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
__table_name__ = 'SUMMER_JUNE22_CORE'
__col_name__ = 'se_report_pdf_status'
headers = {
  'x-hasura-admin-secret': __mentor_report_hasura_secret__,
  'Content-Type': 'application/json'
}
payload = """mutation insert_visit_reports{
      insert_visit_reports(
        objects: [
          {
            mentor_username : "%username",
            creation_date : "%report_date",
            mentor_school : "%mentorschool",
            pdf_url : "%url",
            odk_form_id : "%form"
          }
        ]
        on_conflict: {
          constraint: visit_reports_pkey,
          update_columns: [pdf_url]
        }
      ) {
        affected_rows
        returning {
          mentor_username,
          creation_date,
          mentor_school,
          pdf_url,
          odk_form_id
        }
      }
    }
    """


def get_connection():
    uri = __db_uri__
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def call_pdf_api(uri):
    query_param = {'id': uri}
    url = __pdf_service_url__ + 'summerEngagement'
    r = requests.get(url, params=query_param)
    return r.status_code, r.json()


def update_mentor_pdf_url(pdf_url, username, school, report_date):
    body = payload.replace("%username", username).replace("%report_date", report_date.strftime("%Y-%m-%d")).replace("%url", pdf_url).replace("%mentorschool", school).replace("%form", __table_name__)
    response = requests.request("POST", __mentor_report_hasura_api__, headers=headers, json={"query": body})
    if response.status_code == 200:
        return "success"
    else:
        return "failed"


def update_mentor_report_flag(_uri, cur):
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
    query = 'SELECT * FROM "{}" where EXTRACT(DAY FROM "_MARKED_AS_COMPLETE_DATE") >= {} and EXTRACT(MONTH FROM "_MARKED_AS_COMPLETE_DATE") = {} and EXTRACT(YEAR FROM "_MARKED_AS_COMPLETE_DATE") = {} and {} IS NULL'.format(__table_name__, report_date.day, report_date.month, report_date.year, __col_name__)
    cur.execute(query)
    mentors = cur.fetchall()
    conn.commit()
    conn.close()
    logging.info(f"Total no of records present in {__table_name__} table on {dt_string}: {len(mentors)}")
    for mentor in mentors:
#         time.sleep(2)
        try:
            cur, conn = get_connection()
        except psycopg2.InterfaceError:
            cur, conn = get_connection()
        logging.info(f"For _URI: {mentor['_URI']}")
        status_code, response = call_pdf_api(mentor['_URI'])
        if status_code == 200 and response['status'] != 0:
            logging.info(
                f"{dt_string}: PDF generated successfully for {mentor['_URI']}: Response=> {response['status']}")
            resp = update_mentor_pdf_url(response['pdfs'][0]['url'], mentor['OFFICER_DETAILS_USER_NAME'], mentor['OFFICER_DETAILS_SCHOOL'], mentor['_MARKED_AS_COMPLETE_DATE'])
            if resp == "success":
                update_mentor_report_flag(mentor['_URI'], cur)
                logging.info(f"{dt_string}: PDF status updated for {mentor['_URI']}")
        else:
            logging.info(
                f"{dt_string}: Failed to generate PDF for {mentor['_URI']}: Response=> {response}")
    conn.commit()
    conn.close()
