import logging
from datetime import datetime, timedelta

import psycopg2
import requests
from dateutil.relativedelta import relativedelta
from psycopg2.extras import RealDictCursor

from airflow.models import Variable

logger = logging.getLogger()
logger.setLevel(logging.INFO)
now = datetime.now()


#################################################
#               Configs                         #
#################################################
__db_uri__ = Variable.get("mentor-pdf-data-db")
__sms_api__ = Variable.get("uci-staging-url")

__tablename__ = 'CRCC13M_VF_CORE'

__template_id__ = '1107166556132908879'
__payload__ = """प्रिय शिक्षक, शिक्षा साथी के अनुसार आपके विद्यालय, %school, में हाल ही में मेंटरिंग विज़िट हुई। आशा है की यह विज़िट आपके लिए सहायक रही होगी और आपको अपने मेंटर से महत्वपूर्ण सुझाव मिले होंगे। कृपया मेंटरिंग विज़िट को ले कर अपना अनुभव शेयर करने के लिए  इस लिंक %link पर क्लिक करें और भविष्य में आप जिस विषय पर अपने मेंटर से अधिक सहयोग व मार्गदर्शन चाहेंगे वो भी हमसे शेयर करें। धन्यवाद। समग्र शिक्षा, हिमाचल प्रदेश"""
__baselink__ = 'https://e-sm.in/'
__form_id__ = '01'


__phones__ = ["'7042509168'", "'8670847013'", "'7837833100'"]


def get_connection(uri=__db_uri__):
    '''
    Initiate db connection
    '''
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def call_sms_api(phone_number, template_id, payload):
    body = {
        'adapterId': '4e0c568c-7c42-4f88-b1d6-392ad16b8546',
        'to': {
            'userID': f'{phone_number}',
            'deviceType': 'PHONE',
            'meta': {
                'templateId': f'{template_id}'
            }
        },
        'payload': {
            'text': f'{payload}'
        }
    }
    logging.info(f"Request: {body}")
    r = requests.post(__sms_api__ + '/message/send', json=body, headers={
        'Content-Type': 'application/json'
    })
    return r.status_code, r.json()


def handle_sms_send(**context):
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(days=1)

    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()

    query = f"""
        select * from "{__tablename__}"
        where "_CREATION_DATE"::DATE = '{now.date()}'
        and ("FORM_LOUT_FORM_HINDI_TEACHER_INTERCTON1_TEACHER_MOBILE_H" IN ({','.join(__phones__)})
            or "FORM_LOUT_FORM_ENGLISH_TEACHER_INTERCTON_TEACHER_MOBILE_E" IN ({','.join(__phones__)}))
    """
    cur.execute(query)
    result = cur.fetchall()
    logging.info(f"Total no of feedback on {now.date()}: {len(result)}")

    for feedback in result:
        if feedback['FORM_LOUT_FORM_HINDI_TEACHER_INTERCTON1_TEACHER_MOBILE_H']:
            phone = feedback['FORM_LOUT_FORM_HINDI_TEACHER_INTERCTON1_TEACHER_MOBILE_H']
        elif feedback['FORM_LOUT_FORM_ENGLISH_TEACHER_INTERCTON_TEACHER_MOBILE_E']:
            phone = feedback['FORM_LOUT_FORM_ENGLISH_TEACHER_INTERCTON_TEACHER_MOBILE_E']
        else:
            continue

        body = __payload__.replace('%school', feedback['FORM_LAYOUT_SCHOOL']).replace(
            '%link', (__baselink__ + feedback['_CREATION_DATE'].strftime('%d%m%y') + __form_id__))

        status, resp = call_sms_api(phone, __template_id__, body)
        logging.info(
            f"""SMS request has been initiated for _URI: {feedback['_URI']} with phonenumber {phone}""")
        # if status == 200:
        #     logging.info(
        #         f"""SMS has been sent to _URI: {feedback['_URI']} with phonenumber {phone}""")
        # else:
        #     logging.info(
        #         f"""Failed to send SMS for _URI: {feedback['_URI']} with phonenumber {phone}""")
    conn.commit()
    conn.close()
