import itertools
import requests
import json
import psycopg2
import logging
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from psycopg2.extras import RealDictCursor
from airflow.models import Variable
from pprint import pprint
import pandas as pd

from deps.mentoring_forms.common_mapping import validate_data, format_date, parse_bool_or_other, check_for_subject

logger = logging.getLogger()
logger.setLevel(logging.INFO)
now = datetime.now()

#################################################
#               Configs                         #
#################################################
__db_uri__ = Variable.get("saksham-edu-aggregate")
__pdf_service_url__ = 'http://68.183.94.187:8000'
__mentor_report_hasura_api__ = Variable.get("mentor-report-hasura-api")
__mentor_report_hasura_secret__ = Variable.get("mentor-report-hasura-secret")
__table_name__ = 'ELM3_JNE23_CORE'
__col_name__ = 'monthly_report_pdf_status'
__DOC_GEN_CONFIG_ID__ = 1
__DOC_GEN_TEMPLATE_ID__ = 72

last_month_date = now.replace(day=1) - timedelta(days=1)
__MONTH__ = str(last_month_date.month)
__YEAR__ = str(last_month_date.year)

__DEFAULT_SCHOOL_TARGET__ = 5
__DEFAULT_CLASS_TARGET__ = 13

headers = {
    'x-hasura-admin-secret': __mentor_report_hasura_secret__,
    'Content-Type': 'application/json'
}
payload = """mutation insert_monthly_school_visit_reports{
      insert_monthly_school_visit_reports(
        objects: [
          {
            mentor_username : "%username",
            creation_date : "%report_date",
            pdf_url : "%url"
          }
        ]
        on_conflict: {
          constraint: MONTHLY_SCHOOL_VISIT_REPORTS_pkey,
          update_columns: [pdf_url]
        }
      ) {
        affected_rows
        returning {
          mentor_username,
          creation_date,
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


def call_pdf_api(data_dict):
    data = {
        'config_id': __DOC_GEN_CONFIG_ID__,
        'data': data_dict,
        'template_id': __DOC_GEN_TEMPLATE_ID__
    }
    url = __pdf_service_url__ + '/generate/?plugin=pdf'
    r = requests.post(url, json=data)
    return r.status_code, r.json()


def update_mentor_pdf_url(pdf_url, username, report_date):
    body = payload.replace("%username", username).replace(
        "%report_date", report_date.strftime("%Y-%m-%d")).replace("%url", pdf_url)
    response = requests.request(
        "POST", __mentor_report_hasura_api__, headers=headers, json={"query": body})
    if response.status_code == 200:
        return "success"
    else:
        return "failed"


def update_mentor_report_flag(uris, cur):
    query = """
        UPDATE "{}" SET {}='{}'
        WHERE "_URI" IN ({})
        """.format(__table_name__, __col_name__, 'pdf generated', ','.join(list(map(lambda x: f"'{x}'", uris))))
    cur.execute(query)


def get_school_code(school_name):
    payload = """query MyQuery {
        school(where: {
            school_name: {
                _eq: "%schoolname"
            }
        }) {
            school_code
        }
    }
    """
    body = payload.replace('%schoolname', school_name)
    response = requests.request(
        "POST", __mentor_report_hasura_api__, headers=headers, json={"query": body})
    if response.status_code == 200:
        if response.json()['data']['school']:
            return response.json()['data']['school'][0]['school_code']
    return None


def get_targets(month, year):
    payload = """query MyQuery {
        visit_tracker(where: {month: {_eq: %month}, year: {_eq: %year}}) {
            username
            class_target
            school_target
        }
    }
    """
    body = payload.replace('%month', month).replace('%year', year)
    response = requests.request(
        "POST", __mentor_report_hasura_api__, headers=headers, json={"query": body})
    if response.status_code == 200:
        return response.json()['data']['visit_tracker']
    else:
        return []


def get_from_grade_map(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "Class 4"
    elif item == '2':
        return "Class 5"
    elif item == '3':
        return "Class 6"
    elif item == '4':
        return "Class 7"
    elif item == '5':
        return "Class 8"
    else:
        return item


def get_from_subject_map(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "English"
    elif item == '2':
        return "Hindi"
    elif item == '3':
        return "Math"
    elif item == '4':
        return "EVS"
    elif item == '5':
        return "Science"
    elif item == '6':
        return "SST"
    else:
        return item


def create_pdf_request(**context):
    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()

    report_date = dt_string - relativedelta(months=1)
    query = """
        SELECT * FROM "{}"
        WHERE EXTRACT(MONTH FROM "SCHOOL_VISIT_VISIT_DATE") = {} 
        and EXTRACT(YEAR FROM "SCHOOL_VISIT_VISIT_DATE") = {} 
        and "_MARKED_AS_COMPLETE_DATE" IS NOT NULL 
        AND "{}" IS NULL""".format(__table_name__, __MONTH__, __YEAR__, __col_name__)
    cur.execute(query)
    mentors = cur.fetchall()
    logging.info(
        f"Total no of records present in {__table_name__} table on {dt_string}: {len(mentors)}")
    if not mentors:
        return

    mentors = pd.DataFrame(mentors)
    mentors_data_groups = mentors.groupby(by='OFFICER_DETAILS_USER_NAME')

    targets_listing = get_targets(__MONTH__, __YEAR__)

    for username, data in mentors_data_groups:
        data = data.to_dict(orient='records')
        logging.info(f"For mentor: {username}")

        total_classes_covered = sum(
            int(x['SCHOOL_VISIT_TOTALCLASSES'] if x['SCHOOL_VISIT_TOTALCLASSES'] else 0) for x in data)

        mentor_target = list(filter(
            lambda x: x['username'] == username, targets_listing))
        school_target = __DEFAULT_SCHOOL_TARGET__
        class_target = __DEFAULT_CLASS_TARGET__

        if mentor_target:
            target = list(mentor_target)[0]
            school_target = target['school_target']
            class_target = target['class_target']

        pdf_data = {
            'month': last_month_date.strftime('%B'),
            'year': last_month_date.strftime('%Y'),
            'mentor_username': validate_data(username),
            'total_visits': len(data),
            'total_unique_visits': len(set(x['OFFICER_DETAILS_SCHOOL'] for x in data)),
            'sch_compliance': f"{int((len(data) / (1 if school_target == 0 else school_target)) * 100)}%",
            'total_covered': total_classes_covered,
            'class_compliance': f"{int((total_classes_covered / (1 if class_target == 0 else class_target)) * 100)}%",
        }

        index = 1
        for dataitem in data:
            school_code = get_school_code(dataitem['OFFICER_DETAILS_SCHOOL'])
            if dataitem['CLASS1_DETAILS_GRADE1_PHY']:
                pdf_data.update({
                    f'code{index}': school_code,
                    f'sch_name{index}': validate_data(dataitem['OFFICER_DETAILS_SCHOOL']),
                    f'visit_date{index}': format_date(dataitem['SCHOOL_VISIT_VISIT_DATE']),
                    f'class{index}': get_from_grade_map(dataitem['CLASS1_DETAILS_GRADE1_PHY']),
                    f'subject{index}': get_from_subject_map(dataitem['CLASS1_DETAILS_SUBJECT1_PHY']),
                    f'teacher_name{index}': validate_data(dataitem['CLASS1_DETAILS_NAME1_PHY']),
                    f'feedback{index}': validate_data(dataitem['CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_FEEDBACKDETAILS11_PHY']),
                })
                index += 1

            if dataitem['CLASS2_DETAILS_GRADE2_PHY']:
                pdf_data.update({
                    f'code{index}': school_code,
                    f'sch_name{index}': validate_data(dataitem['OFFICER_DETAILS_SCHOOL']),
                    f'visit_date{index}': format_date(dataitem['SCHOOL_VISIT_VISIT_DATE']),
                    f'class{index}': get_from_grade_map(dataitem['CLASS2_DETAILS_GRADE2_PHY']),
                    f'subject{index}': get_from_subject_map(dataitem['CLASS2_DETAILS_SUBJECT2_PHY']),
                    f'teacher_name{index}': validate_data(dataitem['CLASS2_DETAILS_NAME2_PHY']),
                    f'feedback{index}': validate_data(dataitem['CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_FEEDBACKDETAILS21_PHY']),
                })
                index += 1

            if dataitem['CLASS3_DETAILS_GRADE3_PHY']:
                pdf_data.update({
                    f'code{index}': school_code,
                    f'sch_name{index}': validate_data(dataitem['OFFICER_DETAILS_SCHOOL']),
                    f'visit_date{index}': format_date(dataitem['SCHOOL_VISIT_VISIT_DATE']),
                    f'class{index}': get_from_grade_map(dataitem['CLASS3_DETAILS_GRADE3_PHY']),
                    f'subject{index}': get_from_subject_map(dataitem['CLASS3_DETAILS_SUBJECT3_PHY']),
                    f'teacher_name{index}': validate_data(dataitem['CLASS3_DETAILS_NAME3_PHY']),
                    f'feedback{index}': validate_data(dataitem['CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_FEEDBACKDETAILS31_PHY']),
                })
                index += 1

        status_code, response = call_pdf_api(pdf_data)
        if status_code == 200:
            logging.info(
                f"{dt_string}: PDF generated successfully for mentor {username}: Response=> {response['data']}")
            resp = update_mentor_pdf_url(
                response['data'], validate_data(username), datetime(year=int(__YEAR__), month=int(__MONTH__), day=1) + relativedelta(day=31))
            if resp == "success":
                uris = list(map(lambda x: x['_URI'], data))
                update_mentor_report_flag(uris, cur)
                logging.info(
                    f"{dt_string}: PDF status updated for mentor {username}")
            else:
                logging.info(
                    f"{dt_string}: Failed to generate PDF for mentor {username}: Response=> {response}")
        else:
            logging.info(
                f"{dt_string}: Failed to generate PDF for mentor {username}: Response=> {response}")
        conn.commit()
    conn.commit()
    conn.close()
