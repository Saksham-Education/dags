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
from requests.auth import HTTPDigestAuth
import xmltodict

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
__odk_form_id__ = 'monthlyform_v1'
__table_name__ = 'MNTHLFORMV1_CORE'
__col_name__ = 'monthly_report_pdf_status'
__DOC_GEN_CONFIG_ID__ = 1

odk_url = Variable.get("odk_saksham_url")
odk_username = Variable.get("odk_saksham_username")
odk_password = Variable.get("odk_saksham_password")

odk_preview_service_url = 'https://attachments.samiksha.samagra.io'
odk_preview_service_config_id = 'bf918eeb-1538-4c85-9a1b-808817aed01b'

last_month_date = now.replace(day=1) - timedelta(days=1)
# TODO: Remove below statement when live
last_month_date = now.replace(day=2) - timedelta(days=1)
__MONTH__ = str(last_month_date.month)
__YEAR__ = str(last_month_date.year)


__TEMPLATE_MAPPING__ = [
    {
        'id': 79,
        'wing': 'pre_service'
    },
    {
        'id': 76,
        'wing': "in_service"
    },
    {
        'id': 82,
        'wing': 'dru'
    },
    {
        'id': 78,
        'wing': "edtech"
    },
    {
        'id': 81,
        'wing': 'curriculum_assessment'
    },
    {
        'id': 77,
        'wing': 'planning'
    },
    {
        'id': 80,
        'wing': "work_ex"
    },
]


headers = {
    'x-hasura-admin-secret': __mentor_report_hasura_secret__,
    'Content-Type': 'application/json'
}
payload = """mutation insert_diet_monthly_reports{
      insert_diet_monthly_reports(
        objects: [
          {
            mentor_username : "%username",
            creation_date : "%report_date",
            wing : "%wing",
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


def call_pdf_api(data_dict, template_id):
    data = {
        'config_id': __DOC_GEN_CONFIG_ID__,
        'data': data_dict,
        'template_id': template_id
    }
    url = __pdf_service_url__ + '/generate/?plugin=pdf'
    r = requests.post(url, json=data)
    return r.status_code, r.json()


def update_mentor_pdf_url(pdf_url, username, wing, report_date, submission_date):
    body = payload.replace("%username", username).replace("%report_date", report_date.strftime("%Y-%m-%d")).replace(
        "%url", pdf_url).replace("%wing", wing).replace("%form", __odk_form_id__).replace("%submission_date", str(submission_date))
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


def get_from_designation_map(item):
    if item is None:
        return "NA"
    elif item == 'lecturer':
        return "Lecturer"
    elif item == 'wing_head':
        return "Wing-head"
    elif item == 'principal':
        return "Principal"
    else:
        return item


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
    dump_data['dru_upload_report'] = get_attachment_link(
        odk_submission, 'druwing', 'dru_upload_report')
    dump_data['cmde_upload_report'] = get_attachment_link(
        odk_submission, 'cmdewing', 'cmde_upload_report')
    dump_data['et_upload_report'] = get_attachment_link(
        odk_submission, 'etwing', 'et_upload_report')
    dump_data['ifsc_upload_report'] = get_attachment_link(
        odk_submission, 'ifsc', 'ifsc_upload_report')
    dump_data['pm_upload_report'] = get_attachment_link(
        odk_submission, 'planningandmanagement', 'pm_upload_report')
    dump_data['pste_upload_report'] = get_attachment_link(
        odk_submission, 'pstewing', 'pste_upload_report')
    dump_data['we_upload_report'] = get_attachment_link(
        odk_submission, 'wewing', 'we_upload_report')

    body = """mutation insert_diet_monthly_data_mutation {
        insert_diet_monthly_data_one(object: {
            cmde_pedagogy_trainings: %cmde_pedagogy_trainings, 
            cmde_refreshers_contentdev: %cmde_refreshers_contentdev, 
            cmde_studentassessmenttools: %cmde_studentassessmenttools, 
            cmde_teacherassessmenttools: %cmde_teacherassessmenttools, 
            cmde_tlm: %cmde_tlm, 
            cmde_trainings_tablets: %cmde_trainings_tablets, 
            cmde_upload_report: %cmde_upload_report, 
            designation: %designation, 
            dru_awarenessprog_actual: %dru_awarenessprog_actual, 
            dru_awarenessprog_planned: %dru_awarenessprog_planned, 
            dru_upload_report: %dru_upload_report, 
            et_grievancesrecorded: %et_grievancesrecorded, 
            et_grievancesresolved: %et_grievancesresolved, 
            et_socialmedia_updates: %et_socialmedia_updates, 
            et_tlm: %et_tlm, 
            et_trainings_actual: %et_trainings_actual, 
            et_trainings_cybersafety: %et_trainings_cybersafety, 
            et_trainings_digcontent: %et_trainings_digcontent, 
            et_trainings_planned: %et_trainings_planned, 
            et_trainings_prashnavali: %et_trainings_prashnavali, 
            et_trainings_samiksha: %et_trainings_samiksha, 
            et_trainings_tablets: %et_trainings_tablets,
            et_upload_report: %et_upload_report, 
            et_website_updates: %et_website_updates, 
            ifsc_astermentor_meetings: %ifsc_astermentor_meetings, 
            ifsc_direct_interventions: %ifsc_direct_interventions, 
            ifsc_grievances_recorded: %ifsc_grievances_recorded, 
            ifsc_grievances_resolved: %ifsc_grievances_resolved, 
            ifsc_induction_trainings: %ifsc_induction_trainings, 
            ifsc_leadership_trainings: %ifsc_leadership_trainings, 
            ifsc_mentor_meetings: %ifsc_mentor_meetings, 
            ifsc_refresher_trainings: %ifsc_refresher_trainings, 
            ifsc_research_trainings: %ifsc_research_trainings, 
            ifsc_scertmandated_trainings: %ifsc_scertmandated_trainings, 
            ifsc_teacher_attendance100: %ifsc_teacher_attendance100, 
            ifsc_teacher_attendance50: %ifsc_teacher_attendance50, 
            ifsc_tna: %ifsc_tna, 
            ifsc_tnabased_trainings: %ifsc_tnabased_trainings, 
            ifsc_trainings_actual: %ifsc_trainings_actual, 
            ifsc_trainings_planned: %ifsc_trainings_planned, 
            ifsc_unique_school_visits: %ifsc_unique_school_visits, 
            ifsc_upload_report: %ifsc_upload_report, 
            instance_id: %instance_id, 
            month: %month, 
            name: %name, 
            pm_leadershiptrainings_actual: %pm_leadershiptrainings_actual,
            pm_leadershiptrainings_planned: %pm_leadershiptrainings_planned, 
            pm_trainings_planned: %pm_trainings_planned, 
            pm_trainings_actual: %pm_trainings_actual, 
            pm_upload_report: %pm_upload_report, 
            pste_careercounsellingsessions: %pste_careercounsellingsessions, 
            pste_careercounsellingsessions_students: %pste_careercounsellingsessions_students, 
            pste_mentoringsessions: %pste_mentoringsessions, 
            pste_monitoringvisits: %pste_monitoringvisits, 
            pste_schools_sip: %pste_schools_sip, 
            pste_schoolscovered_mentalhealth: %pste_schoolscovered_mentalhealth, 
            pste_trainings_mentalhealth: %pste_trainings_mentalhealth, 
            pste_upload_report: %pste_upload_report, 
            username: %username, 
            we_trainings_vocational_actual: %we_trainings_vocational_actual, 
            we_trainings_vocational_planned: %we_trainings_vocational_planned, 
            we_upload_report: %we_upload_report, 
            wingname: %wingname, 
            year: %year
        }) {
            instance_id
        }
    }
    """

    body = replace(body, '%cmde_pedagogy_trainings',
                   dump_data['cmde_pedagogy_trainings'])
    body = replace(body, '%cmde_refreshers_contentdev',
                   dump_data['cmde_refreshers_contentdev'])
    body = replace(body, '%cmde_studentassessmenttools',
                   dump_data['cmde_studentassessmenttools'])
    body = replace(body, '%cmde_teacherassessmenttools',
                   dump_data['cmde_teacherassessmenttools'])
    body = replace(body, '%cmde_tlm', dump_data['cmde_tlm'])
    body = replace(body, '%cmde_trainings_tablets',
                   dump_data['cmde_trainings_tablets'])
    body = replace(body, '%cmde_upload_report',
                   dump_data['cmde_upload_report'])
    body = replace(body, '%designation', dump_data['designation'])
    body = replace(body, '%dru_awarenessprog_actual',
                   dump_data['dru_awarenessprog_actual'])
    body = replace(body, '%dru_awarenessprog_planned',
                   dump_data['dru_awarenessprog_planned'])
    body = replace(body, '%dru_upload_report', dump_data['dru_upload_report'])
    body = replace(body, '%et_grievancesrecorded',
                   dump_data['et_grievancesrecorded'])
    body = replace(body, '%et_grievancesresolved',
                   dump_data['et_grievancesresolved'])
    body = replace(body, '%et_socialmedia_updates',
                   dump_data['et_socialmedia_updates'])
    body = replace(body, '%et_tlm', dump_data['et_tlm'])
    body = replace(body, '%et_trainings_actual',
                   dump_data['et_trainings_actual'])
    body = replace(body, '%et_trainings_cybersafety',
                   dump_data['et_trainings_cybersafety'])
    body = replace(body, '%et_trainings_digcontent',
                   dump_data['et_trainings_digcontent'])
    body = replace(body, '%et_trainings_planned',
                   dump_data['et_trainings_planned'])
    body = replace(body, '%et_trainings_prashnavali',
                   dump_data['et_trainings_prashnavali'])
    body = replace(body, '%et_trainings_samiksha',
                   dump_data['et_trainings_samiksha'])
    body = replace(body, '%et_trainings_tablets',
                   dump_data['et_trainings_tablets'])
    body = replace(body, '%et_upload_report', dump_data['et_upload_report'])
    body = replace(body, '%et_website_updates',
                   dump_data['et_website_updates'])
    body = replace(body, '%ifsc_astermentor_meetings',
                   dump_data['ifsc_astermentor_meetings'])
    body = replace(body, '%ifsc_direct_interventions',
                   dump_data['ifsc_direct_interventions'])
    body = replace(body, '%ifsc_grievances_recorded',
                   dump_data['ifsc_grievances_recorded'])
    body = replace(body, '%ifsc_grievances_resolved',
                   dump_data['ifsc_grievances_resolved'])
    body = replace(body, '%ifsc_induction_trainings',
                   dump_data['ifsc_induction_trainings'])
    body = replace(body, '%ifsc_leadership_trainings',
                   dump_data['ifsc_leadership_trainings'])
    body = replace(body, '%ifsc_mentor_meetings',
                   dump_data['ifsc_mentor_meetings'])
    body = replace(body, '%ifsc_refresher_trainings',
                   dump_data['ifsc_refresher_trainings'])
    body = replace(body, '%ifsc_research_trainings',
                   dump_data['ifsc_research_trainings'])
    body = replace(body, '%ifsc_scertmandated_trainings',
                   dump_data['ifsc_scertmandated_trainings'])
    body = replace(body, '%ifsc_teacher_attendance100',
                   dump_data['ifsc_teacher_attendance100'])
    body = replace(body, '%ifsc_teacher_attendance50',
                   dump_data['ifsc_teacher_attendance50'])
    body = replace(body, '%ifsc_tna', dump_data['ifsc_tna'])
    body = replace(body, '%ifsc_tnabased_trainings',
                   dump_data['ifsc_tnabased_trainings'])
    body = replace(body, '%ifsc_trainings_actual',
                   dump_data['ifsc_trainings_actual'])
    body = replace(body, '%ifsc_trainings_planned',
                   dump_data['ifsc_trainings_planned'])
    body = replace(body, '%ifsc_unique_school_visits',
                   dump_data['ifsc_unique_school_visits'])
    body = replace(body, '%ifsc_upload_report',
                   dump_data['ifsc_upload_report'])
    body = replace(body, '%instance_id', dump_data['instance_id'])
    body = replace(body, '%month', dump_data['month'])
    body = replace(body, '%name', dump_data['name'])
    body = replace(body, '%pm_leadershiptrainings_actual',
                   dump_data['pm_leadershiptrainings_actual'])
    body = replace(body, '%pm_leadershiptrainings_planned',
                   dump_data['pm_leadershiptrainings_planned'])
    body = replace(body, '%pm_trainings_planned',
                   dump_data['pm_trainings_planned'])
    body = replace(body, '%pm_trainings_actual',
                   dump_data['pm_trainings_actual'])
    body = replace(body, '%pm_upload_report', dump_data['pm_upload_report'])
    body = replace(body, '%pste_careercounsellingsessions',
                   dump_data['pste_careercounsellingsessions'])
    body = replace(body, '%pste_careercounsellingsessions_students',
                   dump_data['pste_careercounsellingsessions_students'])
    body = replace(body, '%pste_mentoringsessions',
                   dump_data['pste_mentoringsessions'])
    body = replace(body, '%pste_monitoringvisits',
                   dump_data['pste_monitoringvisits'])
    body = replace(body, '%pste_schools_sip', dump_data['pste_schools_sip'])
    body = replace(body, '%pste_schoolscovered_mentalhealth',
                   dump_data['pste_schoolscovered_mentalhealth'])
    body = replace(body, '%pste_trainings_mentalhealth',
                   dump_data['pste_trainings_mentalhealth'])
    body = replace(body, '%pste_upload_report',
                   dump_data['pste_upload_report'])
    body = replace(body, '%username', dump_data['username'])
    body = replace(body, '%we_trainings_vocational_actual',
                   dump_data['we_trainings_vocational_actual'])
    body = replace(body, '%we_trainings_vocational_planned',
                   dump_data['we_trainings_vocational_planned'])
    body = replace(body, '%we_upload_report', dump_data['we_upload_report'])
    body = replace(body, '%wingname', dump_data['wingname'])
    body = replace(body, '%year', dump_data['year'])

    response = requests.request(
        "POST", __mentor_report_hasura_api__, headers=headers, json={"query": body})
    if response.status_code == 200:
        return "success"
    else:
        return "failed"


def get_attachment_link(odk_submission, group, attachment_key):
    attachment_name = odk_submission['data']['data']['wingwiseform'][group][attachment_key]
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
    query = """
        SELECT * FROM "{}" 
        WHERE EXTRACT(MONTH FROM "_SUBMISSION_DATE") = {} 
        and EXTRACT(YEAR FROM "_SUBMISSION_DATE") = {} 
        and "_MARKED_AS_COMPLETE_DATE" IS NOT NULL 
        and "{}" IS NULL
        """.format(__table_name__, __MONTH__, __YEAR__, __col_name__)
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
            'month': extract_month_from_date(mentor['SELECT_DATE_MONTH']),
            'year': '2023',
            'name': validate_data(mentor['SELECT_NAME_NAME']),
            'designation': get_from_designation_map(mentor['SELECT_NAME_DESIGNATION']),
            'wingname': get_from_wing_map(mentor['SELECT_WINGNAME_WINGNAME']),

            'ifsc_unique_school_visits': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_UNIQUE_SCHOOL_VISITS']),
            'ifsc_mentor_meetings': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_MENTOR_MEETINGS']),
            'ifsc_astermentor_meetings': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_ASTERMENTOR_MEETINGS']),
            'ifsc_grievances_recorded': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_GRIEVANCES_RECORDED']),
            'ifsc_grievances_resolved': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_GRIEVANCES_RESOLVED']),
            'ifsc_tna': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_TNA']),
            'ifsc_trainings_planned': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_TRAININGS_PLANNED']),
            'ifsc_trainings_actual': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_TRAININGS_ACTUAL']),
            'ifsc_research_trainings': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_RESEARCH_TRAININGS']),
            'ifsc_scertmandated_trainings': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_SCERTMANDATED_TRAININGS']),
            'ifsc_tnabased_trainings': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_TNABASED_TRAININGS']),
            'ifsc_leadership_trainings': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_LEADERSHIP_TRAININGS']),
            'ifsc_induction_trainings': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_INDUCTION_TRAININGS']),
            'ifsc_refresher_trainings': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_REFRESHER_TRAININGS']),
            'ifsc_direct_interventions': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_DIRECT_INTERVENTIONS']),
            'ifsc_teacher_attendance50': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_TEACHER_ATTENDANCE50']),
            'ifsc_teacher_attendance100': validate_data(mentor['WINGWISEFORM_IFSC_IFSC_TEACHER_ATTENDANCE100']),

            'pm_trainings_planned': validate_data(mentor['WINGWSEFORM_PLANNINGANDMANAGMENT_PM_TRAININGS_PLANNED']),
            'pm_trainings_actual': validate_data(mentor['WINGWSEFORM_PLANNINGANDMANAGMENT_PM_TRAININGS_ACTUAL']),
            'pm_leadershiptrainings_planned': validate_data(mentor['WINGWSEFORM_PLANNINGANDMANAGMENT_PM_LEADERSHIPTRAININGS_PLANNED']),
            'pm_leadershiptrainings_actual': validate_data(mentor['WINGWSEFORM_PLANNINGANDMANAGMENT_PM_LEADERSHIPTRAININGS_ACTUAL']),

            'et_tlm': validate_data(mentor['WINGWISEFORM_ETWING_ET_TLM']),
            'et_trainings_planned': validate_data(mentor['WINGWISEFORM_ETWING_ET_TRAININGS_PLANNED']),
            'et_trainings_actual': validate_data(mentor['WINGWISEFORM_ETWING_ET_TRAININGS_ACTUAL']),
            'et_trainings_digcontent': validate_data(mentor['WINGWISEFORM_ETWING_ET_TRAININGS_DIGCONTENT']),
            'et_trainings_samiksha': validate_data(mentor['WINGWISEFORM_ETWING_ET_TRAININGS_SAMIKSHA']),
            'et_trainings_tablets': validate_data(mentor['WINGWISEFORM_ETWING_ET_TRAININGS_TABLETS']),
            'et_trainings_prashnavali': validate_data(mentor['WINGWISEFORM_ETWING_ET_TRAININGS_PRASHNAVALI']),
            'et_trainings_cybersafety': validate_data(mentor['WINGWISEFORM_ETWING_ET_TRAININGS_CYBERSAFETY']),
            'et_grievancesrecorded': validate_data(mentor['WINGWISEFORM_ETWING_ET_GRIEVANCESRECORDED']),
            'et_grievancesresolved': validate_data(mentor['WINGWISEFORM_ETWING_ET_GRIEVANCESRESOLVED']),
            'et_website_updates': validate_data(mentor['WINGWISEFORM_ETWING_ET_WEBSITE_UPDATES']),
            'et_socialmedia_updates': validate_data(mentor['WINGWISEFORM_ETWING_ET_SOCIALMEDIA_UPDATES']),

            'pste_schools_sip': validate_data(mentor['WINGWISEFORM_PSTEWING_PSTE_SCHOOLS_SIP']),
            'pste_mentoringsessions': validate_data(mentor['WINGWISEFORM_PSTEWING_PSTE_MENTORINGSESSIONS']),
            'pste_monitoringvisits': validate_data(mentor['WINGWISEFORM_PSTEWING_PSTE_MONITORINGVISITS']),
            'pste_careercounsellingsessions': validate_data(mentor['WINGWISEFORM_PSTEWING_PSTE_CAREERCOUNSELLINGSESSIONS']),
            'pste_careercounsellingsessions_students': validate_data(mentor['WINGWISEFORM_PSTEWING_PSTE_CAREERCOUNSELLINGSESSIONS_STUDENTS']),
            'pste_trainings_mentalhealth': validate_data(mentor['WINGWISEFORM_PSTEWING_PSTE_TRAININGS_MENTALHEALTH']),
            'pste_schoolscovered_mentalhealth': validate_data(mentor['WINGWISEFORM_PSTEWING_PSTE_SCHOOLSCOVERED_MENTALHEALTH']),

            'we_trainings_vocational_planned': validate_data(mentor['WINGWISEFORM_WEWING_WE_TRAININGS_VOCATIONAL_PLANNED']),
            'we_trainings_vocational_actual': validate_data(mentor['WINGWISEFORM_WEWING_WE_TRAININGS_VOCATIONAL_ACTUAL']),

            'cmde_pedagogy_trainings': validate_data(mentor['WINGWISEFORM_CMDEWING_CMDE_PEDAGOGY_TRAININGS']),
            'cmde_refreshers_contentdev': validate_data(mentor['WINGWISEFORM_CMDEWING_CMDE_REFRESHERS_CONTENTDEV']),
            'cmde_trainings_tablets': validate_data(mentor['WINGWISEFORM_CMDEWING_CMDE_TRAININGS_TABLETS']),
            'cmde_tlm': validate_data(mentor['WINGWISEFORM_CMDEWING_CMDE_TLM']),
            'cmde_studentassessmenttools': validate_data(mentor['WINGWISEFORM_CMDEWING_CMDE_STUDENTASSESSMENTTOOLS']),
            'cmde_teacherassessmenttools': validate_data(mentor['WINGWISEFORM_CMDEWING_CMDE_TEACHERASSESSMENTTOOLS']),

            'dru_awarenessprog_planned': validate_data(mentor['WINGWISEFORM_DRUWING_DRU_AWARENESSPROG_PLANNED']),
            'dru_awarenessprog_actual': validate_data(mentor['WINGWISEFORM_DRUWING_DRU_AWARENESSPROG_ACTUAL']),
        }

        logging.info(f"For _URI: {mentor['_URI']}")

        dump_response = dump_data_into_hasura(pdf_data, mentor)
        if dump_response == 'success':
            logging.info(
                f"{dt_string}: Data successfully dumped in hasura for {mentor['_URI']}")
        else:
            logging.info(
                f"{dt_string}: Data failed to dumped in hasura for {mentor['_URI']}")

        template = list(filter(
            lambda x: x['wing'] == mentor['SELECT_WINGNAME_WINGNAME'], __TEMPLATE_MAPPING__))[0]
        status_code, response = call_pdf_api(pdf_data, template['id'])
        if status_code == 200:
            logging.info(
                f"{dt_string}: PDF generated successfully for {mentor['_URI']}: Response=> {response}")
            resp = update_mentor_pdf_url(response['data'], validate_data(mentor['USER_DETAILS_USER_NAME']), map_wing_to_shortform(
                mentor['SELECT_WINGNAME_WINGNAME']), mentor['SELECT_DATE_MONTH'], mentor['_SUBMISSION_DATE'])
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
