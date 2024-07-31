from functools import reduce
import logging
from datetime import datetime, timedelta
import re

import psycopg2
import requests
from dateutil.relativedelta import relativedelta
from psycopg2.extras import RealDictCursor
from itertools import groupby
from operator import itemgetter
from urllib.parse import urlencode

from airflow.models import Variable

logger = logging.getLogger()
logger.setLevel(logging.INFO)
now = datetime.now()

#################################################
#               Configs                         #
#################################################
__db_uri__ = Variable.get("hgp_samarth_mv_cred_prod")
__sms_api__ = Variable.get("cdac-url")

__template_id_for_grade_1_to_3__ = '1007023851741156037'
__payload_for_grade_1_to_3__ = "नमस्कार, प्रिय अभिभावक! %assessmenttype में %name का परिणाम - गणित में %mathematics, अंग्रेजी में %english और हिंदी में %hindi है । कृपया %name के शिक्षक से शीघ्र ही मिलें और परिणाम पर चर्चा करें। - e-Samwad\n"

__template_id_for_grade_4_to_5__ = '1007434997413174470'
__payload_for_grade_4_to_5__ = "नमस्कार, प्रिय अभिभावक, %assessmenttype में %name का परिणाम - गणित में %mathematics, अंग्रेजी में %english, हिंदी में %hindi है और पर्यायवरण विज्ञान में %environmentalstudies है । कृपया %name के शिक्षक से शीघ्र ही मिलें और परिणाम पर चर्चा करें। - e-Samwad\n"

__template_id_for_grade_6_to_8__ = '1007481715047657742'
__payload_for_grade_6_to_8__ = "नमस्कार, प्रिय अभिभावक! %assessmenttype में %name का परिणाम - गणित में %mathematics, अंग्रेजी में %english, हिंदी में %hindi, और विज्ञान में %science और सामाजिक विज्ञान में %socialscience है । कृपया %name के शिक्षक से शीघ्र ही मिलें और परिणाम पर चर्चा करें। - e-Samwad\n"

__test_school_id__ = 15547


def get_connection(uri=__db_uri__):
    '''
    Initiate db connection
    '''
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def call_sms_api(phone_number, template_id, payload):
    data = urlencode(
        {
            "message":payload,
            "mobileNumber":phone_number,
            "templateid": template_id
        }
        )
    url = f'{__sms_api__}/api/send_single_unicode_sms?{data}'
    r = requests.get(url=url)
    return r.status_code, r.text


def handle_exam_result(**context):
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(days=1)

    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()

    query = f"""
        select
            MAX(ss.created) as created,
            g."number" as grade,
            ss.school_id,
            a."type" as assessment,
            ss.student_id, 
            st.phone,
            st.name, 
            ss.subject_id, 
            cs.assessment_marks, 
            sj."name" as subject_name,
            c.max_marks as max_marks
        from "student_submission_v2" as ss
        join "assessment" as a on a.id = ss.assessment_id
        join "student" as st on st.id = ss.student_id
        join "student_submission_v2_marks_submissions" as ssms on ssms.studentsubmission_v2_id = ss.id
        join "component_submission" as cs on cs.id = ssms.componentsubmission_id
        join "subject" as sj on sj.id = cs.subject_id
        join "component" as c on c.id = cs.component_id
        join "grade" as g on g.id = ss.grade_id
        where a."type" in ('SA1', 'SA2', 'FA1', 'FA2', 'FA3', 'FA4')
        and ss.created::date = '{now.date()}'
        and ss.school_id = {__test_school_id__}
        GROUP by g."number", ss.school_id, a."type", ss.student_id, st.phone, st.name, ss.subject_id, cs.assessment_marks, sj."name", c.max_marks
    """

    cur.execute(query)
    results = cur.fetchall()
    logging.info(
        f"Total no of submissions present on {now.date()}: {len(results)}")

    q = f"""
        select COUNT(DISTINCT(ss.student_id, ss.school_id, ss.assessment_id)) from "student_submission_v2" as ss
        join "assessment" as a on a.id = ss.assessment_id
        where a."type" in ('SA1', 'SA2', 'FA1', 'FA2', 'FA3', 'FA4')
        and ss.school_id = {__test_school_id__}
        and ss.created::date = '{now.date()}'
    """
    cur.execute(q)
    r = cur.fetchone()
    if r:
        logging.info(
            f"Total no of student's assessment exist on {now.date()}: {r['count']}")

    grouper = itemgetter('student_id', 'school_id', 'assessment')
    results_group = groupby(results, key=grouper)

    for student_detail, student_assessment_data in results_group:
        student_assessment_data_list = list(student_assessment_data)

        name = student_assessment_data_list[0]['name']
        grade = student_assessment_data_list[0]['grade']
        phone = student_assessment_data_list[0]['phone']
        assessment_type = student_detail[2]

        subject_group_marks = groupby(student_assessment_data_list,
                                      key=lambda x: x['subject_id'])

        result = []
        for subject_id, subject_group_row in subject_group_marks:
            result.append(reduce(lambda a, b: {'subject_name': a['subject_name'], 'assessment_marks': a['assessment_marks'] +
                          b['assessment_marks'], 'max_marks': a['max_marks'] + b['max_marks']}, subject_group_row))

        if grade in range(1, 4):
            template_id = __template_id_for_grade_1_to_3__
            sms_content = __payload_for_grade_1_to_3__
        elif grade in range(4, 6):
            template_id = __template_id_for_grade_4_to_5__
            sms_content = __payload_for_grade_4_to_5__
        elif grade in range(6, 9):
            template_id = __template_id_for_grade_6_to_8__
            sms_content = __payload_for_grade_6_to_8__
        else:
            continue

        sms_content = sms_content.replace('%name', name).replace(
            '%assessmenttype', assessment_type)

        # create mapping of subject-marks
        subject_marks_map = dict()
        for r in result:
            keyname = ''.join(r['subject_name'].split()).lower()
            subject_marks_map[keyname] = f"{r['assessment_marks']}/{r['max_marks']}"

        # replace variable in template with values
        for k, v in subject_marks_map.items():
            sms_content = sms_content.replace('%' + k, v)

        # replace variable in template with 'NA' for subject that are missing
        sms_content = re.sub(r'\%[A-z0-9_-]+', 'NA', sms_content)

        status_code, content = call_sms_api(phone, template_id, sms_content)
        if status_code == 200:
            logging.info(
                f"Exam result SMS has been sent for student '{name}' whose phone number is {phone} on {now}")
        else:
            logging.info(
                f"Exam result SMS failed to send for student '{name}' whose phone number is {phone} on {now}")

    conn.commit()
    conn.close()
