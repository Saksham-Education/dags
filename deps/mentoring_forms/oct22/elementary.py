import requests
import json
import psycopg2
import logging
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from psycopg2.extras import RealDictCursor
from airflow.models import Variable

from deps.mentoring_forms.common_mapping import validate_data, format_date, parse_bool_or_other, check_for_subject

logger = logging.getLogger()
logger.setLevel(logging.INFO)
now = datetime.now()

#################################################
#               Configs                         #
#################################################
__db_uri__ = Variable.get("mentor-pdf-data-db")
__pdf_service_url__ = 'http://10.0.160.91:8000'
__mentor_report_hasura_api__ = Variable.get("mentor-report-hasura-api")
__mentor_report_hasura_secret__ = Variable.get("mentor-report-hasura-secret")
__table_name__ = 'LM_OCT22_CORE'
__col_name__ = 'daily_report_pdf_status'
__DOC_GEN_CONFIG_ID__ = 1
__DOC_GEN_TEMPLATE_ID__ = 50

__LM_OCT22_TLMTYPE1_PHY__ = 'LM_OCT22_CLAS1DTLSCLAS1CLASBSRVPHY_TLMTYPE1_PHY'
__LM_OCT22_TLMTYPE2_PHY__ = 'LM_OCT22_CLAS2DTLSCLAS2CLASBSRVPHY_TLMTYPE2_PHY'
__LM_OCT22_TLMTYPE3_PHY__ = 'LM_OCT22_CLAS3DTLSCLAS3CLASBSRVPHY_TLMTYPE3_PHY'

__LM_OCT22_STU_AWARENESS_1__ = 'LM_OCT22_CLASS1_DTILS_CLSS1_STDENT_STU_AWARENESS_1'
__LM_OCT22_STU_AWARENESS_2__ = 'LM_OCT22_CLASS2_DTILS_CLSS2_STDENT_STU_AWARENESS_2'
__LM_OCT22_STU_AWARENESS_3__ = 'LM_OCT22_CLASS3_DTILS_CLSS3_STDENT_STU_AWARENESS_3'

__LM_OCT22_TEACHER_AWARENESS__ = 'LM_OCT22_PHYSCLDTLSSTTEDGTLLRNNGPHY_TEACHER_AWARENESS'
__LM_OCT22_AWARENESS_UDAAN1__ = 'LM_OCT22_PHYSCLDTAILS_STATE_DN_PHY_AWARENESS_UDAAN1'
__LM_OCT22_REMEDIAL_PERIOD__ = 'LM_OCT22_PHYSCLDTAILS_STATE_DN_PHY_REMEDIAL_PERIOD'
__LM_OCT22_SCHOOLSUPP_AREAS_PHY__ = 'LM_OCT22_VST_RPRTNG_SCHL_FDBCK_PHY_SCHOOLSUPP_AREAS_PHY'

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
        return "5 - Excellent (Homework completd properly by all students)"
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
        return "1.5 hours per day"
    elif item == '4':
        return "2 hours per day"
    elif item == '5':
        return "More than 2 hours per day"
    else:
        return item


def get_from_remedial_day_map(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "1 day per week"
    elif item == '2':
        return "2 days  per week"
    elif item == '3':
        return "3 days per week"
    elif item == '4':
        return "4 days per week"
    elif item == '5':
        return "5 days per week"
    elif item == '6':
        return "6 days per week"
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
            result.append('Total time to be devoted for Udaan / remedial')
        elif item == '2':
            result.append('Timetable to be made for Udaan / remedial')
        elif item == '3':
            result.append('Digital books on DIKSHA')
        elif item == '4':
            result.append('Digital books on SCERT')
        elif item == '5':
            result.append('Competencies of previous 2 grades are covered')
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
    conn.commit()
    conn.close()
    logging.info(
        f"Total no of records present in {__table_name__} table on {dt_string}: {len(mentors)}")
    for mentor in mentors:
        try:
            cur, conn = get_connection()
        except psycopg2.InterfaceError:
            cur, conn = get_connection()

        pdf_data = {
            'form_date': format_date(mentor['_MARKED_AS_COMPLETE_DATE']),
            'visit_date': format_date(mentor['SCHOOL_VISIT_VISIT_DATE']),
            'school': validate_data(mentor['OFFICER_DETAILS_NAME']),
            'district': validate_data(mentor['OFFICER_DETAILS_DISTRICT']),
            'block': validate_data(mentor['OFFICER_DETAILS_BLOCK']),
            'grade1_phy': get_from_grade_map(mentor['CLASS1_DETAILS_GRADE1_PHY']),
            'grade2_phy': get_from_grade_map(mentor['CLASS2_DETAILS_GRADE2_PHY']),
            'grade3_phy': get_from_grade_map(mentor['CLASS3_DETAILS_GRADE3_PHY']),
            'section1_phy': validate_data(mentor['CLASS1_DETAILS_SECTION1_PHY']),
            'section2_phy': validate_data(mentor['CLASS2_DETAILS_SECTION2_PHY']),
            'section3_phy': validate_data(mentor['CLASS3_DETAILS_SECTION3_PHY']),
            'subject1_phy': check_for_subject(mentor['CLASS1_DETAILS_SUBJECT1_PHY']),
            'subject2_phy': check_for_subject(mentor['CLASS2_DETAILS_SUBJECT2_PHY']),
            'subject3_phy': check_for_subject(mentor['CLASS3_DETAILS_SUBJECT3_PHY']),
            'name1_phy': validate_data(mentor['CLASS1_DETAILS_NAME1_PHY']),
            'name2_phy': validate_data(mentor['CLASS2_DETAILS_NAME2_PHY']),
            'name3_phy': validate_data(mentor['CLASS3_DETAILS_NAME3_PHY']),
            'engagement_level1': get_from_engagement_map(mentor['CLASS1_DETAILS_CLASS1_CLASS_OBSERVE_PHY_ENGAGEMENT_LEVEL1']),
            'engagement_level2': get_from_engagement_map(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_ENGAGEMENT_LEVEL2']),
            'engagement_level3': get_from_engagement_map(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_ENGAGEMENT_LEVEL3']),
            'comprecord1_phy': get_from_comprecord_map(mentor['CLASS1_DETAILS_CLASS1_CLASS_OBSERVE_PHY_TEACHERWA1_PHY']),
            'comprecord2_phy': get_from_comprecord_map(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_TEACHERWA2_PHY']),
            'comprecord3_phy': get_from_comprecord_map(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_TEACHERWA3_PHY']),
            'comptracker1_phy': parse_bool_or_other(mentor['CLASS1_DETAILS_CLASS1_CLASS_OBSERVE_PHY_COMPTRACKER1_PHY']),
            'comptracker2_phy': parse_bool_or_other(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_COMPTRACKER2_PHY']),
            'comptracker3_phy': parse_bool_or_other(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_COMPTRACKER3_PHY']),
            'tlmtype1_phy': get_multi_tlmtype(cur, mentor['_URI'], __LM_OCT22_TLMTYPE1_PHY__),
            'tlmtype2_phy': get_multi_tlmtype(cur, mentor['_URI'], __LM_OCT22_TLMTYPE2_PHY__),
            'tlmtype3_phy': get_multi_tlmtype(cur, mentor['_URI'], __LM_OCT22_TLMTYPE3_PHY__),
            'tlmusage1_phy': validate_data(mentor['CLASS1_DETAILS_CLASS1_CLASS_OBSERVE_PHY_TLMUSAGE1_PHY']),
            'tlmusage2_phy': validate_data(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_TLMUSAGE2_PHY']),
            'tlmusage3_phy': validate_data(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_TLMUSAGE3_PHY']),
            'teacherwa1_phy': parse_bool_or_other(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_TEACHERWA2_PHY']),
            'teacherwa2_phy': parse_bool_or_other(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_TEACHERWA2_PHY']),
            'teacherwa3_phy': parse_bool_or_other(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_TEACHERWA3_PHY']),
            'stu_grouping1': validate_data(mentor['CLASS1_DETAILS_CLASS1_CLASS_OBSERVE_PHY_STU_GROUPING1']),
            'stu_grouping2': validate_data(mentor['CLASS2_DETAILS_CLASS2_CLASS_OBSERVE_PHY_STU_GROUPING2']),
            'stu_grouping3': validate_data(mentor['CLASS3_DETAILS_CLASS3_CLASS_OBSERVE_PHY_STU_GROUPING3']),
            'ques1_class1_fln': validate_data(mentor['CLASS1_DETAILS_CLASS1_STUDENT_QUES1_CLASS1_FLN']),
            'ques1_class2_fln': validate_data(mentor['CLASS2_DETAILS_CLASS2_STUDENT_QUES1_CLASS2_FLN']),
            'ques1_class3_fln': validate_data(mentor['CLASS3_DETAILS_CLASS3_STUDENT_QUES1_CLASS3_FLN']),
            'ques2_class1_fln': validate_data(mentor['CLASS1_DETAILS_CLASS1_STUDENT_QUES2_CLASS1_FLN']),
            'ques2_class2_fln': validate_data(mentor['CLASS2_DETAILS_CLASS2_STUDENT_QUES2_CLASS2_FLN']),
            'ques2_class3_fln': validate_data(mentor['CLASS3_DETAILS_CLASS3_STUDENT_QUES2_CLASS3_FLN']),
            'ques1_class1_phy': validate_data(mentor['CLASS1_DETAILS_CLASS1_STUDENT_QUES1_CLASS1_PHY']),
            'ques1_class2_phy': validate_data(mentor['CLASS2_DETAILS_CLASS2_STUDENT_QUES1_CLASS2_PHY']),
            'ques1_class3_phy': validate_data(mentor['CLASS3_DETAILS_CLASS3_STUDENT_QUES1_CLASS3_PHY']),
            'ques2_class1_phy': validate_data(mentor['CLASS1_DETAILS_CLASS1_STUDENT_QUES2_CLASS1_PHY']),
            'ques2_class2_phy': validate_data(mentor['CLASS2_DETAILS_CLASS2_STUDENT_QUES2_CLASS2_PHY']),
            'ques2_class3_phy': validate_data(mentor['CLASS3_DETAILS_CLASS3_STUDENT_QUES2_CLASS3_PHY']),
            'stu_awareness_1': get_multi_student_awareness(cur, mentor['_URI'], __LM_OCT22_STU_AWARENESS_1__),
            'stu_awareness_2': get_multi_student_awareness(cur, mentor['_URI'], __LM_OCT22_STU_AWARENESS_2__),
            'stu_awareness_3': get_multi_student_awareness(cur, mentor['_URI'], __LM_OCT22_STU_AWARENESS_3__),
            'stu_homework_1': get_from_student_homework_map(mentor['CLASS1_DETAILS_CLASS1_STUDENT_STU_HOMEWORK_1']),
            'stu_homework_2': get_from_student_homework_map(mentor['CLASS2_DETAILS_CLASS2_STUDENT_STU_HOMEWORK_2']),
            'stu_homework_3': get_from_student_homework_map(mentor['CLASS3_DETAILS_CLASS3_STUDENT_STU_HOMEWORK_3']),
            'teacher_homework_1': get_from_frequency_map(mentor['CLASS1_DETAILS_CLASS1_STUDENT_TEACHER_HOMEWORK_1']),
            'teacher_homework_2': get_from_frequency_map(mentor['CLASS2_DETAILS_CLASS2_STUDENT_TEACHER_HOMEWORK_2']),
            'teacher_homework_3': get_from_frequency_map(mentor['CLASS3_DETAILS_CLASS3_STUDENT_TEACHER_HOMEWORK_3']),
            'teacherfeedback1_phy': parse_bool_or_other(mentor['CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_TEACHERFEEDBACK1_PHY']),
            'teacherfeedback2_phy': parse_bool_or_other(mentor['CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_TEACHERFEEDBACK2_PHY']),
            'teacherfeedback3_phy': parse_bool_or_other(mentor['CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_TEACHERFEEDBACK3_PHY']),
            'feedbackdetails11_phy': validate_data(mentor['CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_FEEDBACKDETAILS11_PHY']),
            'feedbackdetails21_phy': validate_data(mentor['CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_FEEDBACKDETAILS21_PHY']),
            'feedbackdetails31_phy': validate_data(mentor['CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_FEEDBACKDETAILS31_PHY']),
            'schoolhead_phy': parse_bool_or_other(mentor['VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_SCHOOLHEAD_PHY']),
            'schoolhead_name': validate_data(mentor['VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_SCHOOLHEAD_NAME']),
            'schoolsupp_areas_phy': get_multi_schoolsupp_areas(cur, mentor['_URI'], __LM_OCT22_SCHOOLSUPP_AREAS_PHY__),
            'add_school_phy': validate_data(mentor['VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_ADD_SCHOOL_PHY']),
            'improvedif_comments1': validate_data(mentor['VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_IMPROVEDIF_COMMENTS1']),
            'degradedif_comment2': validate_data(mentor['VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_DEGRADEDIF_COMMENT2']),
            'teacher_awareness': get_multi_teacher_awareness(cur, mentor['_URI'], __LM_OCT22_TEACHER_AWARENESS__),
            'diksha_frequency': get_from_frequency_map(mentor['PHYSCLDETAILS_STATE_DIGITAL_LERNNG_PHY_DIKSHA_FREQUENCY']),
            'digitaltools_challenges': validate_data(mentor['PHYSCLDETAILS_STATE_DIGITAL_LERNNG_PHY_DIGITALTOOLS_CHALLENGES']),
            'digitaltools_best': validate_data(mentor['PHYSCLDETAILS_STATE_DIGITAL_LERNNG_PHY_DIGITALTOOLS_BEST']),
            'awareness_udaan': parse_bool_or_other(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_AWARENESS_UDAAN']),
            'awareness_udaan1': get_multi_awareness_udaan1(cur, mentor['_URI'], __LM_OCT22_AWARENESS_UDAAN1__),
            'remedial_period': get_multi_remedial_period(cur, mentor['_URI'], __LM_OCT22_REMEDIAL_PERIOD__),
            'remedial_visit': parse_bool_or_other(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_REMEDIAL_VISIT']),
            'remedial_time': get_from_remedial_timing_map(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_REMEDIAL_TIME']),
            'remedial_day': get_from_remedial_day_map(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_REMEDIAL_DAY']),
            'digital_udaan': get_from_frequency_map(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_DIGITAL_UDAAN']),
            'udaan_challenges': validate_data(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_UDAAN_CHALLENGES']),
            'udaan_best': validate_data(mentor['PHYSICALDETAILS_STATE_UDAAN_PHY_UDAAN_BEST']),
        }

        logging.info(f"For _URI: {mentor['_URI']}")
        status_code, response = call_pdf_api(pdf_data)
        if status_code == 200:
            logging.info(
                f"{dt_string}: PDF generated successfully for {mentor['_URI']}: Response=> {response}")
            resp = update_mentor_pdf_url(response['data'], validate_data(mentor['OFFICER_DETAILS_USER_NAME']),
                                         validate_data(mentor['OFFICER_DETAILS_SCHOOL']), mentor['_MARKED_AS_COMPLETE_DATE'])
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
