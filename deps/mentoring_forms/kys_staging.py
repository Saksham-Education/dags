import itertools
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
__db_uri__ = Variable.get("saksham-edu-aggregate-staging")
__pdf_service_url__ = 'http://68.183.94.187:8000'
__mentor_report_hasura_api__ = Variable.get("mentor-report-hasura-api-staging")
__mentor_report_hasura_secret__ = Variable.get("mentor-report-hasura-secret-staging")
__DOC_GEN_CONFIG_ID__ = 1
__DOC_GEN_TEMPLATE_ID__ = 74

__tables__ = [
    {
        'table': 'LM3_OCT23_CORE',
        'school_support_table': 'LM3_OCT23_VISTRPORTINGSCHOLFDBCKPHY_SCHOOLSUPP_AREAS_PHY',
        'columns': {
            'school': 'OFFICER_DETAILS_SCHOOL',
            'district': 'OFFICER_DETAILS_DISTRICT',
            'block': 'OFFICER_DETAILS_BLOCK',
            'visit_date': 'SCHOOL_VISIT_VISIT_DATE',
            'name1': 'CLASS1_DETAILS_NAME1_PHY',
            'grade1': 'CLASS1_DETAILS_GRADE1_PHY',
            'subject1': 'CLASS1_DETAILS_SUBJECT1_PHY',
            'feedback1': 'CLASS1_DTLS_CLSS1_TECHR_FD_PHY_FEEDBACKDETAILS11_PHY_IMPROVMENT',
            'name2': 'CLASS2_DETAILS_NAME2_PHY',
            'grade2': 'CLASS2_DETAILS_GRADE2_PHY',
            'subject2': 'CLASS2_DETAILS_SUBJECT2_PHY',
            'feedback2': 'CLASS2_DTLS_CLSS2_TECHR_FD_PHY_FEEDBACKDETAILS21_PHY_IMPROVMENT',
            'name3': 'CLASS3_DETAILS_NAME3_PHY',
            'grade3': 'CLASS3_DETAILS_GRADE3_PHY',
            'subject3': 'CLASS3_DETAILS_SUBJECT3_PHY',
            'feedback3': 'CLASS3_DTLS_CLSS3_TECHR_FD_PHY_FEEDBACKDETAILS31_PHY_IMPROVMENT',
            'improved_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_IMPROVEDIF_COMMENTS1',
            'degraded_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_DEGRADEDIF_COMMENT2',
        }
    },
    {
        'table': 'LM2_SEPT23_CORE',
        'school_support_table': 'LM2_SEPT23_VISTRPRTINGSCHLFDBCKPHY_SCHOOLSUPP_AREAS_PHY',
        'columns': {
            'school': 'OFFICER_DETAILS_SCHOOL',
            'district': 'OFFICER_DETAILS_DISTRICT',
            'block': 'OFFICER_DETAILS_BLOCK',
            'visit_date': 'SCHOOL_VISIT_VISIT_DATE',
            'name1': 'CLASS1_DETAILS_NAME1_PHY',
            'grade1': 'CLASS1_DETAILS_GRADE1_PHY',
            'subject1': 'CLASS1_DETAILS_SUBJECT1_PHY',
            'feedback1': 'CLASS1_DTLS_CLSS1_TECHR_FD_PHY_FEEDBACKDETAILS11_PHY_IMPROVMENT',
            'name2': 'CLASS2_DETAILS_NAME2_PHY',
            'grade2': 'CLASS2_DETAILS_GRADE2_PHY',
            'subject2': 'CLASS2_DETAILS_SUBJECT2_PHY',
            'feedback2': 'CLASS2_DTLS_CLSS2_TECHR_FD_PHY_FEEDBACKDETAILS21_PHY_IMPROVMENT',
            'name3': 'CLASS3_DETAILS_NAME3_PHY',
            'grade3': 'CLASS3_DETAILS_GRADE3_PHY',
            'subject3': 'CLASS3_DETAILS_SUBJECT3_PHY',
            'feedback3': 'CLASS3_DTLS_CLSS3_TECHR_FD_PHY_FEEDBACKDETAILS31_PHY_IMPROVMENT',
            'improved_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_IMPROVEDIF_COMMENTS1',
            'degraded_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_DEGRADEDIF_COMMENT2',
        }
    },
    {
        'table': 'ELM3_JNE23_CORE',
        'school_support_table': 'ELM3_JNE23_VISTRPRTINGSCHLFDBCKPHY_SCHOOLSUPP_AREAS_PHY',
        'columns': {
            'school': 'OFFICER_DETAILS_SCHOOL',
            'district': 'OFFICER_DETAILS_DISTRICT',
            'block': 'OFFICER_DETAILS_BLOCK',
            'visit_date': 'SCHOOL_VISIT_VISIT_DATE',
            'name1': 'CLASS1_DETAILS_NAME1_PHY',
            'grade1': 'CLASS1_DETAILS_GRADE1_PHY',
            'subject1': 'CLASS1_DETAILS_SUBJECT1_PHY',
            'feedback1': 'CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_FEEDBACKDETAILS11_PHY',
            'name2': 'CLASS2_DETAILS_NAME2_PHY',
            'grade2': 'CLASS2_DETAILS_GRADE2_PHY',
            'subject2': 'CLASS2_DETAILS_SUBJECT2_PHY',
            'feedback2': 'CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_FEEDBACKDETAILS21_PHY',
            'name3': 'CLASS3_DETAILS_NAME3_PHY',
            'grade3': 'CLASS3_DETAILS_GRADE3_PHY',
            'subject3': 'CLASS3_DETAILS_SUBJECT3_PHY',
            'feedback3': 'CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_FEEDBACKDETAILS31_PHY',
            'improved_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_IMPROVEDIF_COMMENTS1',
            'degraded_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_DEGRADEDIF_COMMENT2',
        }
    },
    {
        'table': 'ELM_MY23_CORE',
        'school_support_table': 'ELM_MY23_VST_RPRTNG_SCHL_FDBCK_PHY_SCHOOLSUPP_AREAS_PHY',
        'columns': {
            'school': 'OFFICER_DETAILS_SCHOOL',
            'district': 'OFFICER_DETAILS_DISTRICT',
            'block': 'OFFICER_DETAILS_BLOCK',
            'visit_date': 'SCHOOL_VISIT_VISIT_DATE',
            'name1': 'CLASS1_DETAILS_NAME1_PHY',
            'grade1': 'CLASS1_DETAILS_GRADE1_PHY',
            'subject1': 'CLASS1_DETAILS_SUBJECT1_PHY',
            'feedback1': 'CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_FEEDBACKDETAILS11_PHY',
            'name2': 'CLASS2_DETAILS_NAME2_PHY',
            'grade2': 'CLASS2_DETAILS_GRADE2_PHY',
            'subject2': 'CLASS2_DETAILS_SUBJECT2_PHY',
            'feedback2': 'CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_FEEDBACKDETAILS21_PHY',
            'name3': 'CLASS3_DETAILS_NAME3_PHY',
            'grade3': 'CLASS3_DETAILS_GRADE3_PHY',
            'subject3': 'CLASS3_DETAILS_SUBJECT3_PHY',
            'feedback3': 'CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_FEEDBACKDETAILS31_PHY',
            'improved_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_IMPROVEDIF_COMMENTS1',
            'degraded_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_DEGRADEDIF_COMMENT2',
        }
    },
    {
        'table': 'LM_APR23_CORE',
        'school_support_table': 'LM_APR23_VST_RPRTNG_SCHL_FDBCK_PHY_SCHOOLSUPP_AREAS_PHY',
        'columns': {
            'school': 'OFFICER_DETAILS_SCHOOL',
            'district': 'OFFICER_DETAILS_DISTRICT',
            'block': 'OFFICER_DETAILS_BLOCK',
            'visit_date': 'SCHOOL_VISIT_VISIT_DATE',
            'name1': 'CLASS1_DETAILS_NAME1_PHY',
            'grade1': 'CLASS1_DETAILS_GRADE1_PHY',
            'subject1': 'CLASS1_DETAILS_SUBJECT1_PHY',
            'feedback1': 'CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_FEEDBACKDETAILS11_PHY',
            'name2': 'CLASS2_DETAILS_NAME2_PHY',
            'grade2': 'CLASS2_DETAILS_GRADE2_PHY',
            'subject2': 'CLASS2_DETAILS_SUBJECT2_PHY',
            'feedback2': 'CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_FEEDBACKDETAILS21_PHY',
            'name3': 'CLASS3_DETAILS_NAME3_PHY',
            'grade3': 'CLASS3_DETAILS_GRADE3_PHY',
            'subject3': 'CLASS3_DETAILS_SUBJECT3_PHY',
            'feedback3': 'CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_FEEDBACKDETAILS31_PHY',
            'improved_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_IMPROVEDIF_COMMENTS1',
            'degraded_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_DEGRADEDIF_COMMENT2',
        }
    },
    {
        'table': 'LM_NOV22_CORE2',
        'school_support_table': 'LM_NOV22_VST_RPRTNG_SCHL_FDBCK_PHY_SCHOOLSUPP_AREAS_PHY',
        'columns': {
            'school': 'OFFICER_DETAILS_SCHOOL',
            'district': 'OFFICER_DETAILS_DISTRICT',
            'block': 'OFFICER_DETAILS_BLOCK',
            'visit_date': 'SCHOOL_VISIT_VISIT_DATE',
            'name1': 'CLASS1_DETAILS_NAME1_PHY',
            'grade1': 'CLASS1_DETAILS_GRADE1_PHY',
            'subject1': 'CLASS1_DETAILS_SUBJECT1_PHY',
            'feedback1': 'CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_FEEDBACKDETAILS11_PHY',
            'name2': 'CLASS2_DETAILS_NAME2_PHY',
            'grade2': 'CLASS2_DETAILS_GRADE2_PHY',
            'subject2': 'CLASS2_DETAILS_SUBJECT2_PHY',
            'feedback2': 'CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_FEEDBACKDETAILS21_PHY',
            'name3': 'CLASS3_DETAILS_NAME3_PHY',
            'grade3': 'CLASS3_DETAILS_GRADE3_PHY',
            'subject3': 'CLASS3_DETAILS_SUBJECT3_PHY',
            'feedback3': 'CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_FEEDBACKDETAILS31_PHY',
            'improved_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_IMPROVEDIF_COMMENTS1',
            'degraded_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_DEGRADEDIF_COMMENT2',
        }
    },
    {
        'table': 'LM_NOV22_CORE',
        'school_support_table': 'LM_NOV22_VST_RPRTNG_SCHL_FDBCK_PHY_SCHOOLSUPP_AREAS_PHY',
        'columns': {
            'school': 'OFFICER_DETAILS_SCHOOL',
            'district': 'OFFICER_DETAILS_DISTRICT',
            'block': 'OFFICER_DETAILS_BLOCK',
            'visit_date': 'SCHOOL_VISIT_VISIT_DATE',
            'name1': 'CLASS1_DETAILS_NAME1_PHY',
            'grade1': 'CLASS1_DETAILS_GRADE1_PHY',
            'subject1': 'CLASS1_DETAILS_SUBJECT1_PHY',
            'feedback1': 'CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_FEEDBACKDETAILS11_PHY',
            'name2': 'CLASS2_DETAILS_NAME2_PHY',
            'grade2': 'CLASS2_DETAILS_GRADE2_PHY',
            'subject2': 'CLASS2_DETAILS_SUBJECT2_PHY',
            'feedback2': 'CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_FEEDBACKDETAILS21_PHY',
            'name3': 'CLASS3_DETAILS_NAME3_PHY',
            'grade3': 'CLASS3_DETAILS_GRADE3_PHY',
            'subject3': 'CLASS3_DETAILS_SUBJECT3_PHY',
            'feedback3': 'CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_FEEDBACKDETAILS31_PHY',
            'improved_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_IMPROVEDIF_COMMENTS1',
            'degraded_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_DEGRADEDIF_COMMENT2',
        }
    },
    {
        'table': 'LM_OCT22_CORE',
        'school_support_table': 'LM_OCT22_VST_RPRTNG_SCHL_FDBCK_PHY_SCHOOLSUPP_AREAS_PHY',
        'columns': {
            'school': 'OFFICER_DETAILS_SCHOOL',
            'district': 'OFFICER_DETAILS_DISTRICT',
            'block': 'OFFICER_DETAILS_BLOCK',
            'visit_date': 'SCHOOL_VISIT_VISIT_DATE',
            'name1': 'CLASS1_DETAILS_NAME1_PHY',
            'grade1': 'CLASS1_DETAILS_GRADE1_PHY',
            'subject1': 'CLASS1_DETAILS_SUBJECT1_PHY',
            'feedback1': 'CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_FEEDBACKDETAILS11_PHY',
            'name2': 'CLASS2_DETAILS_NAME2_PHY',
            'grade2': 'CLASS2_DETAILS_GRADE2_PHY',
            'subject2': 'CLASS2_DETAILS_SUBJECT2_PHY',
            'feedback2': 'CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_FEEDBACKDETAILS21_PHY',
            'name3': 'CLASS3_DETAILS_NAME3_PHY',
            'grade3': 'CLASS3_DETAILS_GRADE3_PHY',
            'subject3': 'CLASS3_DETAILS_SUBJECT3_PHY',
            'feedback3': 'CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_FEEDBACKDETAILS31_PHY',
            'improved_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_IMPROVEDIF_COMMENTS1',
            'degraded_comment': 'VISIT_REPORTING_SCHOOL_FEEDBACK_PHY_DEGRADEDIF_COMMENT2',
        }
    },
    {
        'table': 'LM_SEP22_CORE',
        'school_support_table': 'LM_SEP22_VIST_RPRTNG_SCHOL_FDBCK_PHY_SCHOOLSUPP_AREAS_PHY',
        'columns': {
            'school': 'OFFICER_DETAILS_SCHOOL',
            'district': 'OFFICER_DETAILS_DISTRICT',
            'block': 'OFFICER_DETAILS_BLOCK',
            'visit_date': 'SCHOOL_VISIT_VISIT_DATE',
            'name1': 'CLASS1_DETAILS_NAME1_PHY',
            'grade1': 'CLASS1_DETAILS_GRADE1_PHY',
            'subject1': 'CLASS1_DETAILS_SUBJECT1_PHY',
            'feedback1': 'CLASS1_DETAILS_CLASS1_TEACHER_FEED_PHY_FEEDBACKDETAILS11_PHY',
            'name2': 'CLASS2_DETAILS_NAME2_PHY',
            'grade2': 'CLASS2_DETAILS_GRADE2_PHY',
            'subject2': 'CLASS2_DETAILS_SUBJECT2_PHY',
            'feedback2': 'CLASS2_DETAILS_CLASS2_TEACHER_FEED_PHY_FEEDBACKDETAILS21_PHY',
            'name3': 'CLASS3_DETAILS_NAME3_PHY',
            'grade3': 'CLASS3_DETAILS_GRADE3_PHY',
            'subject3': 'CLASS3_DETAILS_SUBJECT3_PHY',
            'feedback3': 'CLASS3_DETAILS_CLASS3_TEACHER_FEED_PHY_TEACHERFEEDBACK3_PHY',
            'improved_comment': 'VISIT_REPORTING_SCHOOL_IMPROVEMENT_PHY_IMPROVEDIF_COMMENTS1',
            'degraded_comment': 'VISIT_REPORTING_SCHOOL_IMPROVEMENT_PHY_DEGRADEDIF_COMMENT2',
        }
    },

]

headers = {
    'x-hasura-admin-secret': __mentor_report_hasura_secret__,
    'Content-Type': 'application/json'
}
payload = """mutation insert_know_your_school_report{
      insert_know_your_school_report(
        objects: [
          {
            school_code: "%school_code",
            pdf_url: "%url",
            URI_last_visit: %uri_last_visit,
            URI_second_last_visit: %uri_second_last_visit,
            URI_third_last_visit: %uri_third_last_visit
          }
        ]
        on_conflict: {
          constraint: know_your_school_report_pkey,
          update_columns: [pdf_url]
        }
      ) {
        affected_rows
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


def update_mentor_pdf_url(pdf_url, school_code, uri_last_visit, uri_second_last_visit, uri_third_last_visit):
    body = payload.replace(
        "%school_code", school_code).replace("%url", pdf_url)

    if uri_last_visit:
        body = body.replace("%uri_last_visit", f'"{uri_last_visit}"')
    else:
        body = body.replace("%uri_last_visit", "null")

    if uri_second_last_visit:
        body = body.replace("%uri_second_last_visit",
                            f'"{uri_second_last_visit}"')
    else:
        body = body.replace("%uri_second_last_visit", "null")

    if uri_third_last_visit:
        body = body.replace("%uri_third_last_visit",
                            f'"{uri_third_last_visit}"')
    else:
        body = body.replace("%uri_third_last_visit", "null")

    response = requests.request(
        "POST", __mentor_report_hasura_api__, headers=headers, json={"query": body})
    if response.status_code == 200:
        return "success"
    else:
        return "failed"


def get_schools():
    q = """query MyQuery {
        school {
            school_name
            school_code
            district
            block
        }
    }
    """
    response = requests.request(
        "POST", __mentor_report_hasura_api__, headers=headers, json={"query": q})
    if response.status_code == 200:
        return response.json()['data']['school']
    return None


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
    return result


def check_for_ticks(school_support_areas_list, support_name):
    for ssa in school_support_areas_list:
        if ssa.strip().lower() == support_name.strip().lower():
            return '\u2713'
    return ''


def get_last_generated_report(school_code):
    payload = """
        query MyQuery {
            know_your_school_report(
                order_by: {
                    creation_date: desc
                }, 
                where: {
                    school_code: {_eq: "%school_code"}
                }, 
                limit: 1) {
                URI_last_visit
                URI_second_last_visit
                URI_third_last_visit
                creation_date
                pdf_url
                school_code
            }
        }
    """
    body = payload.replace("%school_code", school_code)
    response = requests.request(
        "POST", __mentor_report_hasura_api__, headers=headers, json={"query": body})
    if response.status_code == 200:
        data = response.json()['data']['know_your_school_report']
        if data:
            return data[0]
    return None


def create_pdf_request(**context):
    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(months=1)

    # school query
    schools = get_schools()

    # collect data
    for school in schools:
        pdf_data = {
            'school': school['school_name'],
            'district': school['district'],
            'block': school['block'],
        }

        data = []
        for tableobj in __tables__:
            query = f"""
                SELECT * FROM "{tableobj['table']}"
                WHERE "{tableobj['columns']['school']}" = '{school['school_name']}'
                AND "{tableobj['columns']['district']}" = '{school['district']}'
                AND "{tableobj['columns']['block']}" = '{school['block']}'
                ORDER BY "{tableobj['columns']['visit_date']}" DESC
                LIMIT 3
            """
            cur.execute(query)
            mentors = cur.fetchall()
            logging.info(
                f"Total no of records present in {tableobj['table']} table on {dt_string}: {len(mentors)} for School: {school['school_name']} of Code: {school['school_code']}")
            for mentor in mentors:
                mentor['table'] = tableobj['table']
                data.append(mentor)
                if len(data) == 3:
                    break

        if not data:
            continue

        # check for same pdf
        is_pdf_already_created = True  # assumption
        uri_last_visit = None
        uri_second_last_visit = None
        uri_third_last_visit = None

        try:
            uri_last_visit = data[0]['_URI']
        except:
            pass

        try:
            uri_second_last_visit = data[1]['_URI']
        except:
            pass

        try:
            uri_third_last_visit = data[2]['_URI']
        except:
            pass

        report = get_last_generated_report(school['school_code'])
        if report:
            is_pdf_already_created &= uri_last_visit == report['URI_last_visit']
            is_pdf_already_created &= uri_second_last_visit == report['URI_second_last_visit']
            is_pdf_already_created &= uri_third_last_visit == report['URI_third_last_visit']
        else:
            is_pdf_already_created = False

        # create pdf
        if not is_pdf_already_created:
            row_num = 1
            for i, visit in enumerate(data):
                i = i + 1
                tableobj = list(
                    filter(lambda x: x['table'] == visit['table'], __tables__))[0]

                school_support_areas = get_multi_schoolsupp_areas(
                    cur, visit['_URI'], tableobj['school_support_table'])

                # table1
                for k in range(1, 4):
                    if visit[tableobj['columns'][f'name{k}']]:
                        pdf_data.update({
                            f'r{row_num}_name': validate_data(visit[tableobj['columns'][f'name{k}']]),
                            f'r{row_num}_visit_date': format_date(visit[tableobj['columns']['visit_date']]),
                            f'r{row_num}_grade': get_from_grade_map(visit[tableobj['columns'][f'grade{k}']]),
                            f'r{row_num}_subject': get_from_subject_map(visit[tableobj['columns'][f'subject{k}']]),
                            f'r{row_num}_feedbackdetails': validate_data(visit[tableobj['columns'][f'feedback{k}']]),
                        })
                        row_num += 1

                pdf_data.update({
                    # table2
                    f'slf_r{i}_visit_date': format_date(visit[tableobj['columns']['visit_date']]),
                    f'slf_r{i}_improvedif_comments1': validate_data(visit[tableobj['columns']['improved_comment']]),
                    f'slf_r{i}_degradedif_comment2': validate_data(visit[tableobj['columns']['degraded_comment']]),

                    # table3
                    f'srs_r{i}_visit_date': format_date(visit[tableobj['columns']['visit_date']]),
                    f'srs_r{i}_taws': check_for_ticks(school_support_areas, 'Textbook availability with students'),
                    f'srs_r{i}_pwaws': check_for_ticks(school_support_areas, 'Practice workbook availability with students'),
                    f'srs_r{i}_puta': check_for_ticks(school_support_areas, 'Project Udaan TLM availability'),
                    f'srs_r{i}_ta': check_for_ticks(school_support_areas, 'Teacher availability'),
                    f'srs_r{i}_tt': check_for_ticks(school_support_areas, 'Teacher training'),
                    f'srs_r{i}_bi': check_for_ticks(school_support_areas, 'Better infrastructure'),
                    f'srs_r{i}_trs': check_for_ticks(school_support_areas, 'Tech-related support'),
                    f'srs_r{i}_tshwnaoa': check_for_ticks(school_support_areas, 'The school head was not aware of anything'),
                    f'srs_r{i}_sdnnsia': check_for_ticks(school_support_areas, 'School does not need support in anything'),
                })

            status_code, response = call_pdf_api(pdf_data)
            if status_code == 200:
                logging.info(
                    f"{dt_string}: PDF generated successfully for school {school['school_name']} of Code: {school['school_code']}: Response=> {response['data']} With URIL - {uri_last_visit} : URISL - {uri_second_last_visit} : URITL - {uri_third_last_visit}")
                resp = update_mentor_pdf_url(
                    response['data'], school['school_code'], uri_last_visit, uri_second_last_visit, uri_third_last_visit)
                if resp == "success":
                    logging.info(
                        f"{dt_string}: PDF status updated for school {school['school_name']} of Code: {school['school_code']}")
                else:
                    logging.info(
                        f"{dt_string}: Failed to generate PDF for school {school['school_name']} of Code: {school['school_code']}: Response=> {response}")
            else:
                logging.info(
                    f"{dt_string}: Failed to generate PDF for school {school['school_name']} of Code: {school['school_code']}: Response=> {response}")
    conn.commit()
    conn.close()
