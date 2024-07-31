import requests
import logging
from datetime import datetime
from requests.exceptions import HTTPError
from airflow.models import Variable

logger = logging.getLogger()
logger.setLevel(logging.INFO)
now = datetime.now()

#################################################
#               Configs                         #
#################################################
__hasura_uri__ = Variable.get("mentor-report-hasura-api")
__user_service_url__ = Variable.get("user-service-uri")
__x_hasura_admin_secret__ = Variable.get("mentor-report-hasura-secret")
__application_id__ = 'a664a16a-95dd-41fc-9b9b-e45fb49cf128'

hasura_headers = {
  'x-hasura-admin-secret': __x_hasura_admin_secret__,
  'Content-Type': 'application/json'
}

user_service_headers = {
   'x-application-id': __application_id__,
  'Content-Type': 'application/json'
}

def manage_school_mentor_mapping_request():
    # Fetching all the school mentor mappings from Hasura.
    non_active_mentors = []
    school_mentor_mapping_list = []
    try: 
        requestBody = {
            "query": '''
                query MyQuery {
                    school_mentor_mapping(where: {}) {
                      id
                      mentor_block
                      mentor_mobile
                      mentor_username
                    }
                  }

                ''',
            "variables": {}
        }
        school_mentor_mapping_request = requests.post(
            __hasura_uri__, headers=hasura_headers, json=requestBody)
        jsonResponse = school_mentor_mapping_request.json()
        school_mentor_mapping_list = jsonResponse['data']['school_mentor_mapping']
    except HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err}')
    except Exception as err:
        logging.error(f'Other error occurred: {err}')

    # Finding all the users that are non active or non existant
    for el in school_mentor_mapping_list:
        try:
            logging.info('Checking status of ' + el["mentor_username"])
            mentor_request = requests.get(
            __user_service_url__+'/searchUserByQuery?startRow=0&numberOfResults=1&queryString=(registrations.roles:%20Mentor)AND(username:'+el["mentor_username"]+')', headers=user_service_headers)
            jsonResponse = mentor_request.json()        

            if jsonResponse['result'] is None:
                non_active_mentors.append(el['id'])
            else :
                if jsonResponse['result']['users'][0]['active'] == False:
                    non_active_mentors.append(el['id'])
        except HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')
        except Exception as err:
            logging.error(f'Other error occurred: {err}')

    logging.info("Mentors to remove : ")
    logging.info(non_active_mentors)

    if len(non_active_mentors) :
        for el in non_active_mentors:
            logging.info("Removing mentor " + str(el))
            requestBody = {
                "query": "mutation MyMutation { delete_school_mentor_mapping(where: {id: {_eq: "+str(el)+"}}) { affected_rows } }",
                "variables": {}
            }
            delete_mentor_response = requests.post(
            __hasura_uri__, headers=hasura_headers, json=requestBody)
            jsonResponse = delete_mentor_response.json()
            logging.info(jsonResponse)

# manage_school_mentor_mapping_request()
                