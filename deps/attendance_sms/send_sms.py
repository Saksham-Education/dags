import logging
from datetime import datetime, timedelta

import psycopg2
import requests
from dateutil.relativedelta import relativedelta
from psycopg2.extras import RealDictCursor
from urllib.parse import urlencode
from airflow.models import Variable

logger = logging.getLogger()
logger.setLevel(logging.INFO)
now = datetime.now()


#################################################
#               Configs                         #
#################################################
__db_uri__ = Variable.get("hgp_samarth_mv_cred_prod")
__attendance_table__ = 'attendance'
__attendance_sms_logs_table__ = 'attendance_sms_logs'

__sms_api__ = Variable.get("cdac-url")

__absent_daily_template_id__ = '1007683050485484200'
__absent_daily_payload__ = "प्रिय अभिभावक %studentname, %date को स्कूल में अनुपस्थित थे जिसके कारण उनकी पढ़ाई पर असर पड़ेगा। कृपया उन्हें रोज़ स्कूल भेजना सुनिश्चित करें। समग्र शिक्षा, हिमाचल प्रदेश"

__absent_three_day_multiple_template_id__ = '1007298169987658048'
__absent_three_day_multiple_payload__ = "प्रिय अभिभावक! %studentname, %days दिनों से स्कूल से अनुपस्थित है। कृपया इसका कारण जानने की कोशिश करें और सुनिश्चित करें कि %studentname रोज़ाना स्कूल आए। समग्र शिक्षा, हिमाचल प्रदेश"

__present_template_id__ = '1007205597644362599'
__present_payload__ = "प्रिय अभिभावक! हमें ख़ुशी है कि %studentname दो सप्ताह से नियमित रूप से स्कूल आ रहे हैं। कृपया सुनिश्चित करें कि भविष्य में भी इसी तरह की नियमितता बनाए रखे। समग्र शिक्षा, हिमाचल प्रदेश"

__school_id_less_than_equals__ = 1500
__test_school_id__ = 15547


def get_connection(uri=__db_uri__):
    '''
    Initiate db connection
    '''
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def create_attendance_log(conn, cur, student_id, sms_type, for_days, sent_date):
    '''
    generate attendance sms log for student
    '''
    query = f"""
            insert into "{__attendance_sms_logs_table__}" ("student_id", "sms_type", "for_days", "sent_date")
            values ('{student_id}','{sms_type}','{for_days}','{sent_date}')
        """
    cur.execute(query)
    conn.commit()
    logging.info(
        f"Total row affected in {__attendance_sms_logs_table__} table on {sent_date}: {str(cur.rowcount)}")


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


def send_absent_sms(conn, cur, student_id, name, phone_number, for_days, sms_sent_date=now.date()):
    if for_days % 3 == 0:
        logging.info(
            f"Sending 3 days absent SMS to Student of id {student_id} of name {name} of phone {phone_number}")
        status_code, content = call_sms_api(phone_number, __absent_three_day_multiple_template_id__, __absent_three_day_multiple_payload__.replace(
            '%studentname', name).replace('%days', str(for_days)))
    else:  # send 1 day sms but log it as consecutive in 'attendance_sms_logs' table
        logging.info(
            f"Sending 1 days absent SMS to Student of id {student_id} of name {name} of phone {phone_number}")
        status_code, content = call_sms_api(phone_number, __absent_daily_template_id__, __absent_daily_payload__.replace(
            '%studentname', name).replace('%date', str(sms_sent_date)))
    create_attendance_log(conn, cur, student_id,
                          'absent', for_days, sms_sent_date)


def send_present_sms(conn, cur, student_id, name, phone_number, for_days, sms_sent_date=now.date()):
    logging.info(
        f"Sending 12 days present SMS to Student of id {student_id} of name {name} of phone {phone_number}")
    status_code, content = call_sms_api(
        phone_number, __present_template_id__, __present_payload__.replace('%studentname', name))
    create_attendance_log(conn, cur, student_id,
                          'present', for_days, sms_sent_date)


def handle_absent_sms(**context):
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(days=1)

    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()

    most_recent_attendance_date = now.date()

    query = f"""
        select t1.student_id, s.name, s.phone, t3.sms_type, t3.for_days, t3.sent_date, t1.taken_by_school_id from (
            SELECT student_id, taken_by_school_id FROM "{__attendance_table__}"
            where "date" = '{most_recent_attendance_date}'
            and is_present = false
        ) as t1
        left join (
            SELECT t2.student_id, MAX(asl.sms_type) as sms_type, MAX(asl.for_days) as for_days, MAX(t2.sent_date) as sent_date from "{__attendance_sms_logs_table__}" as asl
            join (
                SELECT student_id, MAX(sent_date) as sent_date from "{__attendance_sms_logs_table__}"
                where sms_type = 'absent'
                group by student_id
            ) as t2
            on t2.student_id = asl.student_id and t2.sent_date = asl.sent_date
            GROUP BY t2.student_id
        ) as t3
        on t1.student_id = t3.student_id
        join student as s on t1.student_id = s.id
        where (t1.taken_by_school_id <= {__school_id_less_than_equals__} or t1.taken_by_school_id = {__test_school_id__})
    """
    cur.execute(query)
    absentees = cur.fetchall()
    logging.info(
        f"Total no of absentees on {most_recent_attendance_date}: {len(absentees)}")

    for student in absentees:
        if student['sent_date'] and student['for_days']:
            delta = most_recent_attendance_date - student['sent_date']
            if delta.days > 2:  # reset counter
                # send 1 day sms
                send_absent_sms(
                    conn, cur, student['student_id'], student['name'], student['phone'], 1, most_recent_attendance_date)

            elif delta.days == 2:
                # if current day is monday and sms_sent_day was saturday then send incremental sms
                __MONDAY__ = 0
                __SATURDAY__ = 5
                # sunday check
                if most_recent_attendance_date.weekday() == __MONDAY__ and student['sent_date'].weekday() == __SATURDAY__:
                    # send 1 day or 3-day multiple sms
                    send_absent_sms(conn, cur, student['student_id'], student['name'],
                                    student['phone'], student['for_days'] + 1, most_recent_attendance_date)
                else:
                    # send 1 day sms
                    send_absent_sms(
                        conn, cur, student['student_id'], student['name'], student['phone'], 1, most_recent_attendance_date)

            elif delta.days == 1:  # we haven't already sent sms for this student
                # send 1 day or 3-day multiple sms
                send_absent_sms(
                    conn, cur, student['student_id'], student['name'], student['phone'], student['for_days'] + 1, most_recent_attendance_date)
        else:
            # send 1 day sms
            send_absent_sms(
                conn, cur, student['student_id'], student['name'], student['phone'], 1, most_recent_attendance_date)

    conn.commit()
    conn.close()


def handle_present_sms(**context):
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(days=1)

    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()

    query = f"""
        select distinct "date" from "{__attendance_table__}" 
        where "date" <= '{now.date()}'
        and (taken_by_school_id <= {__school_id_less_than_equals__} or taken_by_school_id = {__test_school_id__})
        order by "date" desc limit 12
    """
    cur.execute(query)
    result = cur.fetchall()

    if result:
        most_recent_attendance_date = result[0]['date']
    else:
        logging.info(f"No attendance records found")
        exit()

    query = f"""
        select t1.student_id, s.name, s.phone, t1.presents, t3.sms_type, t3.for_days, t3.sent_date, t1.taken_by_school_id from (
            SELECT student_id, count(student_id) as presents, MAX(taken_by_school_id) as taken_by_school_id FROM "{__attendance_table__}"
            where "date" in ({','.join([f"'{x['date']}'" for x in result])})
            and is_present = true
            GROUP by student_id
            HAVING count(student_id) = 12
        ) as t1
        left join (
            SELECT t2.student_id, MAX(asl.sms_type) as sms_type, MAX(asl.for_days) as for_days, MAX(t2.sent_date) as sent_date from "{__attendance_sms_logs_table__}" as asl
            join (
                SELECT student_id, MAX(sent_date) as sent_date from "{__attendance_sms_logs_table__}"
                where sms_type = 'present'
                group by student_id
            ) as t2
            on t2.student_id = asl.student_id and t2.sent_date = asl.sent_date
            GROUP BY t2.student_id
        ) as t3
        on t1.student_id = t3.student_id
        join student as s on t1.student_id = s.id
        where (t1.taken_by_school_id <= {__school_id_less_than_equals__} or t1.taken_by_school_id = {__test_school_id__})
    """
    cur.execute(query)
    consecutive_presentees = cur.fetchall()
    logging.info(
        f"Total no of twelve consecutive days presentees on {most_recent_attendance_date}: {len(consecutive_presentees)}")

    for student in consecutive_presentees:
        # 12-day present sms
        if student['sent_date'] and student['for_days'] and student['for_days'] == 12:
            delta = most_recent_attendance_date - student['sent_date']
            if delta.days >= 12:
                # send 12 day sms
                send_present_sms(
                    conn, cur, student['student_id'], student['name'], student['phone'], 12, most_recent_attendance_date)
        else:
            # send 12 day sms
            send_present_sms(
                conn, cur, student['student_id'], student['name'], student['phone'], 12, most_recent_attendance_date)

    conn.commit()
    conn.close()
    