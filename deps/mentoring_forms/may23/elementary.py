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
__table_name__ = 'ELM_MY23_CORE'
__col_name__ = 'daily_report_pdf_status'
__DOC_GEN_CONFIG_ID__ = 1
__DOC_GEN_TEMPLATE_ID__ = 84

__LM_TLMTYPE1_PHY__ = 'ELM_MY23_CLAS1DTLSCLAS1CLASBSRVPHY_TLMTYPE1_PHY'
__LM_TLMTYPE2_PHY__ = 'ELM_MY23_CLAS2DTLSCLAS2CLASBSRVPHY_TLMTYPE2_PHY'
__LM_TLMTYPE3_PHY__ = 'ELM_MY23_CLAS3DTLSCLAS3CLASBSRVPHY_TLMTYPE3_PHY'

__LM_STU_AWARENESS_1__ = 'ELM_MY23_CLAS1DTLSCLAS1STUDENTWRNES_STU_AWARENESS_1'
__LM_STU_AWARENESS_2__ = 'ELM_MY23_CLAS2DTLSCLAS2STUDENTWRNES_STU_AWARENESS_2'
__LM_STU_AWARENESS_3__ = 'ELM_MY23_CLAS3DTLSCLAS3STUDENTWRNES_STU_AWARENESS_3'

__LM_TEACHER_AWARENESS__ = 'ELM_MY23_PHYSCLDTLSSTTEDGTLLRNNGPHY_TEACHER_AWARENESS'
__LM_AWARENESS_UDAAN1__ = 'ELM_MY23_PHYSCLDTAILS_STATE_DN_PHY_AWARENESS_UDAAN1'
__LM_REMEDIAL_PERIOD__ = 'ELM_MY23_PHYSCLDTAILS_STATE_DN_PHY_REMEDIAL_PERIOD'
__LM_SCHOOLSUPP_AREAS_PHY__ = 'ELM_MY23_VST_RPRTNG_SCHL_FDBCK_PHY_SCHOOLSUPP_AREAS_PHY'

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


def call_pdf_api(data_dict):
    data = {
        'config_id': __DOC_GEN_CONFIG_ID__,
        'data': data_dict,
        'template_id': __DOC_GEN_TEMPLATE_ID__
    }
    url = __pdf_service_url__ + '/generate/?plugin=pdf'
    r = requests.post(url, json=data)
    return r.status_code, r.json()


def update_mentor_pdf_url(pdf_url, username, school, report_date):
    body = payload.replace("%username", username).replace("%report_date", report_date.strftime(
        "%Y-%m-%d")).replace("%url", pdf_url).replace("%mentorschool", school).replace("%form", __table_name__)
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


def get_from_engagement_map(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "Excellent (All students are attentive and most are interacting)"
    elif item == '2':
        return "Good (More than 80% of students are attentive and some are interacting)"
    elif item == '3':
        return "Satisfactory (More than 50% of the students are attentive and some are interacting)"
    elif item == '4':
        return "Dissatisfactory (less than 50% students are attentive and very few students are interacting)"
    elif item == '5':
        return "Extremely dissatisfactory (Students are not paying attention or interacting)"
    else:
        return item


def get_from_comprecord_map(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "2 or more groups"
    elif item == '2':
        return "No grouping done"
    elif item == '3':
        return "Teacher not aware about grouping per learning levels"
    else:
        return item


def get_from_student_homework_map(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "1 - Extremely dissatisfactory (Students have not completed homework)"
    elif item == '2':
        return "2 - Dissatisfactory (Quality of homework done is poor)"
    elif item == '3':
        return "3 - Satisfactory (Homework completed and quality is satisfactory)"
    elif item == '4':
        return "4 - Good (Good quality homework by most students)"
    elif item == '5':
        return "5 - Excellent (Homework completed properly by all students)"
    else:
        return item


def get_from_frequency_map(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "Never"
    elif item == '2':
        return "Rarely (Once or twice in a week)"
    elif item == '3':
        return "Often (3 or 4 times in a week)"
    elif item == '4':
        return "Always"
    else:
        return item


def get_from_remedial_timing_map(item):
    if item is None:
        return "NA"
    elif item == '0':
        return "No remedial classes are conducted"
    elif item == '1':
        return "Less than 1 hour per day"
    elif item == '2':
        return "1 hour per day"
    elif item == '3':
        return "More than 1 hour per day"
    else:
        return item


def get_from_remedial_day_map(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "5 days - One subject per day"
    elif item == '2':
        return "1 day per week"
    elif item == '3':
        return "2 days per week"
    elif item == '4':
        return "3 days per week"
    else:
        return item


def get_multi_tlmtype(cur, uri, table):
    q = f'''
        SELECT * FROM "{table}"
        WHERE "_PARENT_AURI" = '{uri}'
    '''
    cur.execute(q)
    records = cur.fetchall()
    result = []
    for record in records:
        item = record['VALUE']
        if item == '1':
            result.append('Blackboard and chalk or whiteboard and marker')
        elif item == '2':
            result.append('Smartboard / Digital board')
        elif item == '3':
            result.append(
                'Charts, letters, maps, posters, flashcards, banners, toys or story books / big book')
        elif item == '4':
            result.append('Departmental Kits or Teaching aids')
        elif item == '5':
            result.append('Printed Textbook')
        elif item == '6':
            result.append('Workbooks or worksheets / content on DIKSHA')
        elif item == '7':
            result.append('Videos from Youtube')
        elif item == '8':
            result.append('Avsar App')
    if result:
        return ', '.join(result)
    return 'NA'


def get_multi_student_awareness(cur, uri, table):
    q = f'''
        SELECT * FROM "{table}"
        WHERE "_PARENT_AURI" = '{uri}'
    '''
    cur.execute(q)
    records = cur.fetchall()
    result = []
    for record in records:
        item = record['VALUE']
        if item == '0':
            result.append('Student not aware of any of the above')
        elif item == '1':
            result.append('Avsar end of chapter practice quiz')
        elif item == '2':
            result.append('DIKSHA videos')
        elif item == '3':
            result.append('Date of upcoming SAT or other assessment test')
    if result:
        return ', '.join(result)
    return 'NA'


def get_multi_teacher_awareness(cur, uri, table):
    q = f'''
        SELECT * FROM "{table}"
        WHERE "_PARENT_AURI" = '{uri}'
    '''
    cur.execute(q)
    records = cur.fetchall()
    result = []
    for record in records:
        item = record['VALUE']
        if item == '0':
            result.append('Teachers not aware of any of the above')
        elif item == '1':
            result.append('Avsar report card')
        elif item == '2':
            result.append('DIKSHA worksheets')
        elif item == '3':
            result.append('DIKSHA video')
        elif item == '4':
            result.append('Avsar end of chapter practice quiz')
        elif item == '5':
            result.append('FLN agenda of the state')
    if result:
        return ', '.join(result)
    return 'NA'


def get_multi_awareness_udaan1(cur, uri, table):
    q = f'''
        SELECT * FROM "{table}"
        WHERE "_PARENT_AURI" = '{uri}'
    '''
    cur.execute(q)
    records = cur.fetchall()
    result = []
    for record in records:
        item = record['VALUE']
        if item == '0':
            result.append(
                'School Head/ Teachers not aware of any of the above')
        elif item == '1':
            result.append(
                'Total daily time to be devoted for remedial period in the time table')
        elif item == '2':
            result.append('Remedial teaching-learning cycle')
        elif item == '3':
            result.append('Use of physical Udaan Teacher Manual')
        elif item == '4':
            result.append('Use of physical Udaan Student Workbook')
        elif item == '5':
            result.append('Coverage of competencies from previous two grades')
    if result:
        return ', '.join(result)
    return 'NA'


def get_multi_remedial_period(cur, uri, table):
    q = f'''
        SELECT * FROM "{table}"
        WHERE "_PARENT_AURI" = '{uri}'
    '''
    cur.execute(q)
    records = cur.fetchall()
    result = []
    for record in records:
        item = record['VALUE']
        if item == '0':
            result.append('None of the classes')
        elif item == '1':
            result.append('Class 6')
        elif item == '2':
            result.append('Class 7')
        elif item == '3':
            result.append('Class 8')
    if result:
        return ', '.join(result)
    return 'NA'


def get_multi_schoolsupp_areas(cur, uri, table):
    q = f'''
        SELECT * FROM "{table}"
        WHERE "_PARENT_AURI" = '{uri}'
    '''
    cur.execute(q)
    records = cur.fetchall()
    result = []
    for record in records:
        item = record['VALUE']
        if item == '0':
            result.append('School does not need support in anything')
        elif item == '1':
            result.append('Textbook availability with students')
        elif item == '2':
            result.append('Practice workbook availability with students')
        elif item == '3':
            result.append('Project Udaan TLM availability')
        elif item == '4':
            result.append('Teacher availability')
        elif item == '5':
            result.append('Teacher training')
        elif item == '6':
            result.append('Better infrastructure')
        elif item == '7':
            result.append('Tech-related support')
        elif item == '8':
            result.append('The school head was not aware of anything')
    if result:
        return ', '.join(result)
    return 'NA'


def check_for_student_assessment(subject, item):
    if (str(subject) == '6'):
        return validate_data(item)
    else:
        return 'N/A - Spot Assessment via tool'


def get_from_physical_udaanmanual_map(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "Never"
    elif item == '2':
        return "Rarely (Teachers don't use the manual to plan class activities)"
    elif item == '3':
        return "Often (Teachers find manuals useful but need to build content themselves)"
    elif item == '4':
        return "Always (Teachers only follow the manual to plan class activities)"
    else:
        return item


def get_from_physical_udaanworksheets(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "Never (Neither students receive home work nor they do self-practice)"
    elif item == '2':
        return "Rarely (Students are given home-work but do not comply)"
    elif item == '3':
        return "Often (Students are given homework and comply regularly but not for all subjects)"
    elif item == '4':
        return "Always (Teachers prescribe homework and students comply for all subjects)"
    else:
        return item


def create_pdf_request(**context):
    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(days=1)
    query = 'SELECT * FROM "{}" WHERE "_MARKED_AS_COMPLETE_DATE" IS NOT NULL and "{}" IS NULL'.format(
        __table_name__, __col_name__)
    cur.execute(query)
    mentors = cur.fetchall()
    logging.info(
        f"Total no of records present in {__table_name__} table on {dt_string}: {len(mentors)}")
    for mentor in mentors:
        pdf_data = {
            '1': format_date(mentor['_MARKED_AS_COMPLETE_DATE']),
            '2': format_date(mentor['SCHOOL_VISIT_VISIT_DATE']),
            '3': validate_data(mentor['OFFICER_DETAILS_SCHOOL']),
            '4': validate_data(mentor['OFFICER_DETAILS_DISTRICT']),
            '5': validate_data(mentor['OFFICER_DETAILS_BLOCK']),
            '6': get_from_grade_map(mentor['CLASS1_DETAILS_GRADE1_PHY']),
            '7': get_from_grade_map(mentor['CLASS2_DETAILS_GRADE2_PHY']),
            '8': get_from_grade_map(mentor['CLASS3_DETAILS_GRADE3_PHY']),
            '9': validate_data(mentor['CLASS1_DETAILS_SECTION1_PHY']),
            '10': validate_data(mentor['CLASS2_DETAILS_SECTION2_PHY']),
            '11': validate_data(mentor['CLASS3_DETAILS_SECTION3_PHY']),
            '12': check_for_subject(mentor['CLASS1_DETAILS_SUBJECT1_PHY']),
            '13': check_for_subject(mentor['CLASS2_DETAILS_SUBJECT2_PHY']),
            '14': check_for_subject(mentor['CLASS3_DETAILS_SUBJECT3_PHY']),
            '15': validate_data(mentor['CLASS1_DETAILS_NAME1_PHY']),
            '16': validate_data(mentor['CLASS2_DETAILS_NAME2_PHY']),
            '17': validate_data(mentor['CLASS3_DETAILS_NAME3_PHY']),
            '18': get_from_engagement_map(mentor['CLASS1_DETAILS_CLASS1_CLASS_OBSERVE_PHY_ENGAGEMENT_LEVEL1']),
            '19': get_from_engagement_map(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_ENGAGEMENT_LEVEL2']),
            '20': get_from_engagement_map(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_ENGAGEMENT_LEVEL3']),
            '21': get_from_comprecord_map(mentor['CLASS1_DETAILS_CLASS1_CLASS_OBSERVE_PHY_COMPRECORD1_PHY']),
            '22': get_from_comprecord_map(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_COMPRECORD2_PHY']),
            '23': get_from_comprecord_map(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_COMPRECORD3_PHY']),
            '24': parse_bool_or_other(mentor['CLASS1_DETAILS_CLASS1_CLASS_OBSERVE_PHY_COMPTRACKER1_PHY']),
            '25': parse_bool_or_other(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_COMPTRACKER2_PHY']),
            '26': parse_bool_or_other(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_COMPTRACKER3_PHY']),
            '27': get_multi_tlmtype(cur, mentor['_URI'], __LM_TLMTYPE1_PHY__),
            '28': get_multi_tlmtype(cur, mentor['_URI'], __LM_TLMTYPE2_PHY__),
            '29': get_multi_tlmtype(cur, mentor['_URI'], __LM_TLMTYPE3_PHY__),
            '30': validate_data(mentor['CLASS1_DETAILS_CLASS1_CLASS_OBSERVE_PHY_TLMUSAGE1_PHY']),
            '31': validate_data(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_TLMUSAGE2_PHY']),
            '32': validate_data(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_TLMUSAGE3_PHY']),
            '33': parse_bool_or_other(mentor['CLASS1_DETAILS_CLASS1_CLASS_OBSERVE_PHY_TEACHERWA1_PHY']),
            '34': parse_bool_or_other(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_TEACHERWA2_PHY']),
            '35': parse_bool_or_other(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_TEACHERWA3_PHY']),
            '36': validate_data(mentor['CLASS1_DETAILS_CLASS1_CLASS_OBSERVE_PHY_STU_GROUPING1']),
            '37': validate_data(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_STU_GROUPING2']),
            '38': validate_data(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_STU_GROUPING3']),
            '39': check_for_student_assessment(mentor['CLASS1_DETAILS_SUBJECT1_PHY'], mentor['CLASS1_DETAILS_CLASS1_STUDENT_SST_QUES1_CLASS1_PHY']),
            '40': check_for_student_assessment(mentor['CLASS2_DETAILS_SUBJECT2_PHY'], mentor['CLASS2_DETAILS_CLASS2_STUDENT_SST_QUES1_CLASS2_PHY']),
            '41': check_for_student_assessment(mentor['CLASS3_DETAILS_SUBJECT3_PHY'], mentor['CLASS3_DETAILS_CLASS3_STUDENT_SST_QUES1_CLASS3_PHY']),
            '42': check_for_student_assessment(mentor['CLASS1_DETAILS_SUBJECT1_PHY'], mentor['CLASS1_DETAILS_CLASS1_STUDENT_SST_QUES2_CLASS1_PHY']),
            '43': check_for_student_assessment(mentor['CLASS2_DETAILS_SUBJECT2_PHY'], mentor['CLASS2_DETAILS_CLASS2_STUDENT_SST_QUES2_CLASS2_PHY']),
            '44': check_for_student_assessment(mentor['CLASS3_DETAILS_SUBJECT3_PHY'], mentor['CLASS3_DETAILS_CLASS3_STUDENT_SST_QUES2_CLASS3_PHY']),
            '45': check_for_student_assessment(mentor['CLASS1_DETAILS_SUBJECT1_PHY'], mentor['CLASS1_DETAILS_CLASS1_STUDENT_SST_QUES3_CLASS1_PHY']),
            '46': check_for_student_assessment(mentor['CLASS2_DETAILS_SUBJECT2_PHY'], mentor['CLASS2_DETAILS_CLASS2_STUDENT_SST_QUES3_CLASS2_PHY']),
            '47': check_for_student_assessment(mentor['CLASS3_DETAILS_SUBJECT3_PHY'], mentor['CLASS3_DETAILS_CLASS3_STUDENT_SST_QUES3_CLASS3_PHY']),
            '48': get_multi_student_awareness(cur, mentor['_URI'], __LM_STU_AWARENESS_1__),
            '49': get_multi_student_awareness(cur, mentor['_URI'], __LM_STU_AWARENESS_2__),
            '50': get_multi_student_awareness(cur, mentor['_URI'], __LM_STU_AWARENESS_3__),
            '51': get_from_student_homework_map(mentor['CLASS1_DETAILS_CLASS1_STUDENTAWARENESS_STU_HOMEWORK_1']),
            '52': get_from_student_homework_map(mentor['CLASS2_DETAILS_CLASS2_STUDENTAWARENESS_STU_HOMEWORK_2']),
            '53': get_from_student_homework_map(mentor['CLASS3_DETAILS_CLASS3_STUDENTAWARENESS_STU_HOMEWORK_3']),
            '54': get_from_frequency_map(mentor['CLASS1_DETAILS_CLASS1_STUDENTAWARENESS_TEACHER_HOMEWORK_1']),
            '55': get_from_frequency_map(mentor['CLASS2_DETAILS_CLASS2_STUDENTAWARENESS_TEACHER_HOMEWORK_2']),
            '56': get_from_frequency_map(mentor['CLASS3_DETAILS_CLASS3_STUDENTAWARENESS_TEACHER_HOMEWORK_3']),
            '57': parse_bool_or_other(mentor['CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_TEACHERFEEDBACK1_PHY']),
            '58': parse_bool_or_other(mentor['CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_TEACHERFEEDBACK2_PHY']),
            '59': parse_bool_or_other(mentor['CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_TEACHERFEEDBACK3_PHY']),
            '60': validate_data(mentor['CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_FEEDBACKDETAILS11_PHY']),
            '61': validate_data(mentor['CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_FEEDBACKDETAILS21_PHY']),
            '62': validate_data(mentor['CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_FEEDBACKDETAILS31_PHY']),
            '63': parse_bool_or_other(mentor['VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_SCHOOLHEAD_PHY']),
            '64': validate_data(mentor['VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_SCHOOLHEAD_NAME']),
            '65': get_multi_schoolsupp_areas(cur, mentor['_URI'], __LM_SCHOOLSUPP_AREAS_PHY__),
            '66': validate_data(mentor['VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_ADD_SCHOOL_PHY']),
            '67': validate_data(mentor['VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_IMPROVEDIF_COMMENTS1']),
            '68': validate_data(mentor['VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_DEGRADEDIF_COMMENT2']),
            '69': get_multi_teacher_awareness(cur, mentor['_URI'], __LM_TEACHER_AWARENESS__),
            '70': get_from_frequency_map(mentor['PHYSCLDETAILS_STATE_DIGITAL_LERNNG_PHY_DIKSHA_FREQUENCY']),
            '71': validate_data(mentor['PHYSCLDETAILS_STATE_DIGITAL_LERNNG_PHY_DIGITALTOOLS_CHALLENGES']),
            '72': validate_data(mentor['PHYSCLDETAILS_STATE_DIGITAL_LERNNG_PHY_DIGITALTOOLS_BEST']),
            '73': parse_bool_or_other(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_AWARENESS_UDAAN']),
            '74': get_multi_remedial_period(cur, mentor['_URI'], __LM_REMEDIAL_PERIOD__),
            '75': get_multi_awareness_udaan1(cur, mentor['_URI'], __LM_AWARENESS_UDAAN1__),
            '76': parse_bool_or_other(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_ACCESS_UDAAN']),
            '77': validate_data(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_REMEDIAL_VISIT']),
            '78': get_from_remedial_timing_map(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_REMEDIAL_TIME']),
            '79': get_from_remedial_day_map(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_REMEDIAL_DAY']),
            '80': get_from_physical_udaanmanual_map(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_PHYSICAL_UDAANMANUAL']),
            '81': get_from_physical_udaanworksheets(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_PHYSICAL_UDAANWORKSHEETS']),
            '82': validate_data(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_UDAAN_CHALLENGES']),
            '83': validate_data(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_UDAAN_BEST']),
        }

        logging.info(f"For _URI: {mentor['_URI']}")
        status_code, response = call_pdf_api(pdf_data)
        if status_code == 200:
            logging.info(
                f"{dt_string}: PDF generated successfully for {mentor['_URI']}: Response=> {response}")
            resp = update_mentor_pdf_url(response['data'], validate_data(mentor['OFFICER_DETAILS_USER_NAME']), validate_data(
                mentor['OFFICER_DETAILS_SCHOOL']), mentor['SCHOOL_VISIT_VISIT_DATE'] if mentor['SCHOOL_VISIT_VISIT_DATE'] else mentor['_SUBMISSION_DATE'])
            if resp == "success":
                update_mentor_report_flag(mentor['_URI'], cur)
                logging.info(
                    f"{dt_string}: PDF status updated for {mentor['_URI']}")
        else:
            logging.info(
                f"{dt_string}: Failed to generate PDF for {mentor['_URI']}: Response=> {response}")
        conn.commit()
    conn.commit()
    conn.close()
