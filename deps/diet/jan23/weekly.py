import requests
import json
import psycopg2
import logging
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from psycopg2.extras import RealDictCursor
from airflow.models import Variable
from pprint import pprint
from requests.auth import HTTPDigestAuth
import xmltodict
import json

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
__odk_form_id__ = 'dietsweeky_v1'
__table_name__ = 'DITSWEKY_V1_CORE'
__col_name__ = 'weekly_report_pdf_status'
__DOC_GEN_CONFIG_ID__ = 1
__DOC_GEN_TEMPLATE_ID__ = 75

odk_url = Variable.get("odk_saksham_url")
odk_username = Variable.get("odk_saksham_username")
odk_password = Variable.get("odk_saksham_password")

odk_preview_service_url = 'https://attachments.samiksha.samagra.io'
odk_preview_service_config_id = 'bf918eeb-1538-4c85-9a1b-808817aed01b'

__SUPPORTWING_V101__ = 'DITSWEKY_V1_WEKLY_FORM_SUOMOTU_PRJECTS_SUPPORTINGWING_V101'
__SUPPORTWING_V102__ = 'DITSWEKY_V1_WEKLFRMSCERTMANDATDPRJECTS_SUPPORTINGWING_V102'
__SUPPORTWING_V103__ = 'DITSWEKY_V1_WEKLFRMPRJCTSASIGNEDTHRTY_SUPPORTINGWING_V103'


headers = {
    'x-hasura-admin-secret': __mentor_report_hasura_secret__,
    'Content-Type': 'application/json'
}
payload = """mutation insert_diet_weekly_reports{
      insert_diet_weekly_reports(
        objects: [
          {
            mentor_username : "%username",
            creation_date : "%report_date",
            wing : "%wing",
            week : "%week",
            pdf_url : "%url",
            odk_form_id : "%form",
            submission_date: "%submission_date"
          }
        ]
      ) {
        affected_rows
        returning {
          mentor_username,
          creation_date,
          wing,
          week
          pdf_url,
          odk_form_id,
          submission_date
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


def update_mentor_pdf_url(pdf_url, username, wing, week, report_date, submission_date):
    body = payload.replace("%username", username).replace("%report_date", report_date.strftime("%Y-%m-%d")).replace("%url", pdf_url).replace(
        "%wing", wing).replace("%week", week).replace("%form", __odk_form_id__).replace("%submission_date", str(submission_date))
    response = requests.request(
        "POST", __mentor_report_hasura_api__, headers=headers, json={"query": body})
    if response.status_code == 200:
        return "success"
    else:
        return "failed"


def update_mentor_report_flag(_uri, cur):
    query = """UPDATE "{}" SET {}='{}' where "_URI"='{}'""".format(
        __table_name__, __col_name__, 'pdf generated', _uri)
    cur.execute(query)


def validate_data(item):
    if item is not None:
        return str(item)
    return 'NA'


def format_date(item):
    if item is None:
        return 'NA'
    if isinstance(item, datetime):
        return item.strftime('%d %B %Y')
    return datetime.strptime(item, '%Y-%m-%d').strftime('%d %B %Y')


def get_from_authroity_map(item):
    if item is None:
        return "NA"
    else:
        return item


def get_from_ws_map(item):
    if item is None:
        return "NA"
    elif item == 'yes':
        return "Yes"
    elif item == 'no':
        return "No"
    else:
        return item


def get_from_top_map(item):
    if item is None:
        return "NA"
    elif item == 'Trainings':
        return "Trainings/Workshops/Seminar/Event"
    elif item == 'Visits':
        return "School Visits/Monitoring"
    elif item == 'Research':
        return "Research/Analysis"
    elif item == 'Other':
        return "Other"
    elif item == 'None':
        return "None"
    else:
        return item


def get_multi_sw_map(cur, uri, table):
    q = f'''
        SELECT * FROM "{table}"
        WHERE "_PARENT_AURI" = '{uri}'
    '''
    cur.execute(q)
    records = cur.fetchall()
    result = []
    for record in records:
        item = record['VALUE']
        if item == 'pre_service':
            result.append("Pre-service Training and Education")
        elif item == 'in_service':
            result.append("In-service Training and Field Interactions")
        elif item == 'dru':
            result.append("District Resource Unit")
        elif item == 'edtech':
            result.append("Education Technology")
        elif item == 'curriculum_assessment':
            result.append("Curriculum, Material Development & Assessment")
        elif item == 'planning':
            result.append("Planning and Management")
        elif item == 'work_ex':
            result.append("Work Experience")
    if result:
        return ', '.join(result)
    return 'NA'


def get_from_wing_map(item):
    if item is None:
        return "NA"
    elif item == 'pre_service':
        return "Pre-service Training and Education"
    elif item == 'in_service':
        return "In-service Training and Field Interactions"
    elif item == 'dru':
        return "District Resource Unit"
    elif item == 'edtech':
        return "Education Technology"
    elif item == 'curriculum_assessment':
        return "Curriculum, Material Development & Assessment"
    elif item == 'planning':
        return "Planning and Management"
    elif item == 'work_ex':
        return "Work Experience"
    else:
        return item


def get_from_week_map(item):
    if item is None:
        return "NA"
    elif item == 'week 1':
        return "W1"
    elif item == 'week 2':
        return "W2"
    elif item == 'week 3':
        return "W3"
    elif item == 'week 4':
        return "W4"
    elif item == 'week 5':
        return "W5"
    else:
        return item


def map_wing_to_shortform(item):
    if item is None:
        return "NA"
    elif item == 'pre_service':
        return "PSTE"
    elif item == 'in_service':
        return "IFIC"
    elif item == 'dru':
        return "DRU"
    elif item == 'edtech':
        return "ET"
    elif item == 'curriculum_assessment':
        return "CMDE"
    elif item == 'planning':
        return "P&M"
    elif item == 'work_ex':
        return "WE"
    else:
        return item


def extract_month_from_date(item):
    if item is None:
        return 'NA'
    if isinstance(item, datetime):
        return item.strftime('%B')
    return datetime.strptime(item, '%Y-%m-%d').strftime('%B')


def extract_year_from_date(item):
    if item is None:
        return 'NA'
    if isinstance(item, datetime):
        return item.strftime('%Y')
    return datetime.strptime(item, '%Y-%m-%d').strftime('%Y')


def get_submission(uuid, form_id):
    auth = HTTPDigestAuth(odk_username, odk_password)
    result = requests.get(f'{odk_url}/view/downloadSubmission?formId={form_id}[@version=null and @uiVersion=null]/data[@key={uuid}]', headers={
        'Content-Type': 'text/xml; charset=utf-8'
    }, auth=auth)

    if result.status_code != 200:
        raise Exception(f'failed to fetch form:{form_id} submissions')

    return xmltodict.parse(result.text)['submission']


def replace(body, original, replacement):
    if replacement is None:
        return body.replace(original, "null", 1)
    else:
        return body.replace(original, f'"{replacement}"', 1)


def dump_data_into_hasura(pdf_data, mentor):
    odk_submission = get_submission(mentor['_URI'], __odk_form_id__)
    dump_data = {}
    for k, v in pdf_data.items():
        dump_data[k] = None if v == 'NA' else v
    dump_data['instance_id'] = mentor['_URI']
    dump_data['suo_pictures'] = get_pictures_link(
        odk_submission, 'suomotu_projects')
    dump_data['scert_pictures'] = get_pictures_link(
        odk_submission, 'scert_mandated_projects')
    dump_data['oa_pictures'] = get_pictures_link(
        odk_submission, 'projects_assigned_authority')

    body = """mutation insert_diet_weekly_data {
        insert_diet_weekly_data_one(object: {
            instance_id: %instance_id, 
            month: %month, 
            oa_authority: %oa_authority, 
            oa_description: %oa_description, 
            oa_misc_undertaken: %oa_misc_undertaken, 
            oa_supportingwing_v103: %oa_supportingwing_v103, 
            oa_typeofprojects_v103: %oa_typeofprojects_v103, 
            oa_wingsupport_v103: %oa_wingsupport_v103, 
            scert_description: %scert_description, 
            scert_supportingwing_v102: %scert_supportingwing_v102, 
            scert_typeofprojects_v102: %scert_typeofprojects_v102, 
            scert_undertaken: %scert_undertaken, 
            scert_wingsupport_v102: %scert_wingsupport_v102, 
            suomotu_description: %suomotu_description, 
            suomotu_supportingwing_v101: %suomotu_supportingwing_v101, 
            suomotu_typeofprojects_v101: %suomotu_typeofprojects_v101, 
            suomotu_undertaken: %suomotu_undertaken, 
            suomotu_wingsupport_v101: %suomotu_wingsupport_v101, 
            total_completed_projects: %total_completed_projects, 
            total_planned_projects: %total_planned_projects, 
            total_started_projects: %total_started_projects, 
            username: %username, 
            week: %week, 
            wingname: %wingname, 
            year: %year,
            suomotu_pictures: %suomotu_pictures,
            scert_pictures: %scert_pictures,
            oa_pictures: %oa_pictures
        }) {
            instance_id
        }
    }
    """

    body = replace(body, '%instance_id', dump_data['instance_id'])
    body = replace(body, '%month', dump_data['month'])
    body = replace(body, '%oa_authority', dump_data['oa_authority'])
    body = replace(body, '%oa_description', dump_data['oa_description'])
    body = replace(body, '%oa_misc_undertaken',
                   dump_data['oa_misc_undertaken'])
    body = replace(body, '%oa_supportingwing_v103',
                   dump_data['oa_supportingwing_v103'])
    body = replace(body, '%oa_typeofprojects_v103',
                   dump_data['oa_typeofprojects_v103'])
    body = replace(body, '%oa_wingsupport_v103',
                   dump_data['oa_wingsupport_v103'])
    body = replace(body, '%scert_description', dump_data['scert_description'])
    body = replace(body, '%scert_supportingwing_v102',
                   dump_data['scert_supportingwing_v102'])
    body = replace(body, '%scert_typeofprojects_v102',
                   dump_data['scert_typeofprojects_v102'])
    body = replace(body, '%scert_undertaken',
                   dump_data['scert_scert_undertaken'])
    body = replace(body, '%scert_wingsupport_v102',
                   dump_data['scert_wingsupport_v102'])
    body = replace(body, '%suomotu_description', dump_data['suo_description'])
    body = replace(body, '%suomotu_supportingwing_v101',
                   dump_data['suo_supportingwing_v101'])
    body = replace(body, '%suomotu_typeofprojects_v101',
                   dump_data['suo_typeofprojects_v101'])
    body = replace(body, '%suomotu_undertaken',
                   dump_data['suo_suomotu_undertaken'])
    body = replace(body, '%suomotu_wingsupport_v101',
                   dump_data['suo_wingsupport_v101'])
    body = replace(body, '%total_completed_projects',
                   dump_data['tp_completed_projects'])
    body = replace(body, '%total_planned_projects',
                   dump_data['tp_planned_projects'])
    body = replace(body, '%total_started_projects',
                   dump_data['tp_started_projects'])
    body = replace(body, '%username', dump_data['username'])
    body = replace(body, '%week', dump_data['week'])
    body = replace(body, '%wingname', dump_data['wingname'])
    body = replace(body, '%year', dump_data['year'])
    body = replace(body, '%suomotu_pictures', dump_data['suo_pictures'])
    body = replace(body, '%scert_pictures', dump_data['scert_pictures'])
    body = replace(body, '%oa_pictures', dump_data['oa_pictures'])

    response = requests.request(
        "POST", __mentor_report_hasura_api__, headers=headers, json={"query": body})
    if response.status_code == 200:
        return "success"
    else:
        return "failed"


def get_pictures_link(odk_submission, attachment_key):
    attachment_name = odk_submission['data']['data']['weekly_form'][attachment_key]['pictures']
    if attachment_name:
        attachment = None
        if isinstance(odk_submission['mediaFile'], list):
            attachment = list(
                filter(lambda x: x['filename'] == attachment_name, odk_submission['mediaFile']))
            if attachment:
                attachment = attachment[0]
        else:
            if odk_submission['mediaFile']['filename'] == attachment_name:
                attachment = odk_submission['mediaFile']
        if attachment:
            return f"{odk_preview_service_url}?url={attachment['downloadUrl']}&filename={attachment['filename']}&configId={odk_preview_service_config_id}"
    return None


def create_pdf_request(**context):
    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(days=1)
    query = '''
        SELECT * FROM "{}" 
        WHERE "_MARKED_AS_COMPLETE_DATE" IS NOT NULL 
        and "{}" IS NULL
        '''.format(__table_name__, __col_name__)
    cur.execute(query)
    mentors = cur.fetchall()

    logging.info(
        f"Total no of records present in {__table_name__} table on {dt_string}: {len(mentors)}")
    for mentor in mentors:
        try:
            cur, conn = get_connection()
        except psycopg2.InterfaceError:
            cur, conn = get_connection()

        pdf_data = {
            'username': validate_data(mentor['USER_DETAILS_USER_NAME']),
            'month': extract_month_from_date(mentor['SELECT_WEEK_MONTH']),
            'year': '2023',
            'week': get_from_week_map(mentor['SELECT_WEEK_WEEK']),
            'wingname': get_from_wing_map(mentor['SELECT_WING_WINGNAME']),
            'tp_planned_projects': validate_data(mentor['WEEKLY_FORM_TOTAL_PROJECTS_PLANNED_PROJECTS']),
            'tp_started_projects': validate_data(mentor['WEEKLY_FORM_TOTAL_PROJECTS_STARTED_PROJECTS']),
            'tp_completed_projects': validate_data(mentor['WEEKLY_FORM_TOTAL_PROJECTS_COMPLETED_PROJECTS']),
            'suo_suomotu_undertaken': validate_data(mentor['WEEKLY_FORM_SUOMOTU_PROJECTS_SUOMOTU_UNDERTAKEN']),
            'suo_typeofprojects_v101': get_from_top_map(mentor['WEEKLY_FORM_SUOMOTU_PROJECTS_TYPEOFPROJECTS_V101']),
            'suo_description': validate_data(mentor['WEEKLY_FORM_SUOMOTU_PROJECTS_DESCRIPTION']),
            'suo_wingsupport_v101': get_from_ws_map(mentor['WEEKLY_FORM_SUOMOTU_PROJECTS_WINGSUPPORT_V101']),
            'suo_supportingwing_v101': get_multi_sw_map(cur, mentor['_URI'], __SUPPORTWING_V101__),
            'scert_scert_undertaken': validate_data(mentor['WEEKLY_FORM_SCERT_MANDATED_PROJECTS_SCERT_UNDERTAKEN']),
            'scert_typeofprojects_v102': get_from_top_map(mentor['WEEKLY_FORM_SCERT_MANDATED_PROJECTS_TYPEOFPROJECTS_V102']),
            'scert_description': validate_data(mentor['WEEKLY_FORM_SCERT_MANDATED_PROJECTS_DESCRIPTION']),
            'scert_wingsupport_v102': get_from_ws_map(mentor['WEEKLY_FORM_SCERT_MANDATED_PROJECTS_WINGSUPPORT_V102']),
            'scert_supportingwing_v102': get_multi_sw_map(cur, mentor['_URI'], __SUPPORTWING_V102__),
            'oa_misc_undertaken': validate_data(mentor['WEEKLY_FORM_PROJECTS_ASSIGNED_AUTHORITY_MISC_UNDERTAKEN']),
            'oa_authority': get_from_authroity_map(mentor['WEEKLY_FORM_PROJECTS_ASSIGNED_AUTHORITY_AUTHORITY']),
            'oa_typeofprojects_v103': get_from_top_map(mentor['WEEKLY_FORM_PROJECTS_ASSIGNED_AUTHORITY_TYPEOFPROJECTS_V103']),
            'oa_description': validate_data(mentor['WEEKLY_FORM_PROJECTS_ASSIGNED_AUTHORITY_DESCRIPTION']),
            'oa_wingsupport_v103': get_from_ws_map(mentor['WEEKLY_FORM_PROJECTS_ASSIGNED_AUTHORITY_WINGSUPPORT_V103']),
            'oa_supportingwing_v103': get_multi_sw_map(cur, mentor['_URI'], __SUPPORTWING_V103__),
        }

        logging.info(f"For _URI: {mentor['_URI']}")

        dump_response = dump_data_into_hasura(pdf_data, mentor)
        if dump_response == 'success':
            logging.info(
                f"{dt_string}: Data successfully dumped in hasura for {mentor['_URI']}")
        else:
            logging.info(
                f"{dt_string}: Data failed to dumped in hasura for {mentor['_URI']}")

        status_code, response = call_pdf_api(pdf_data)
        if status_code == 200:
            logging.info(
                f"{dt_string}: PDF generated successfully for {mentor['_URI']}: Response=> {response}")
            resp = update_mentor_pdf_url(response['data'], validate_data(mentor['USER_DETAILS_USER_NAME']), map_wing_to_shortform(
                mentor['SELECT_WING_WINGNAME']), get_from_week_map(mentor['SELECT_WEEK_WEEK']), mentor['SELECT_WEEK_MONTH'], mentor['_SUBMISSION_DATE'])
            if resp == "success":
                update_mentor_report_flag(mentor['_URI'], cur)
                logging.info(
                    f"{dt_string}: PDF status updated for {mentor['_URI']}")
            else:
                logging.info(
                    f"{dt_string}: Failed to generate PDF for {mentor['_URI']}: Response=> {response}")
        else:
            logging.info(
                f"{dt_string}: Failed to call PDF api for {mentor['_URI']}: Response=> {response}")
        conn.commit()
    conn.commit()
    conn.close()
