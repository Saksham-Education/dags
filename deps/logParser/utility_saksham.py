import io
import sys
from lars import apache, csv
import datetime
import logging
import pandas as pd
import numpy as np
from urllib.parse import unquote
import urllib.request
from bs4 import BeautifulSoup
import requests
import json
from sqlalchemy import create_engine

logger = logging.getLogger()
logger.setLevel(logging.INFO)

LOG_SERVER_URL = "http://165.22.211.88:8080/"

def decode_url(url):
    url = unquote(url)
    return url

def get_log_file():
    log_files_request = urllib.request.urlopen(LOG_SERVER_URL)
    log_files_HTML = log_files_request.read().decode("utf8")
    parsed_html = BeautifulSoup(log_files_HTML, 'html.parser')
    log_url_list = parsed_html.find_all('a')
    log_url_list = [item.get('href').replace("/","") for item in log_url_list]
    current_date = str((datetime.datetime.now()).date()-datetime.timedelta(days=2)).replace("-", "_")
    today_log_file = "access_"+current_date+".log"
    logging.info(f"Day before Yesterday's log file name is {today_log_file}")
    full_file_path = ""
    if  today_log_file in log_url_list:
        full_file_path = LOG_SERVER_URL+today_log_file
        return full_file_path
    else:
        logging.error("No log file exist for Yesterday's date.")
        return full_file_path
    
def log_parser():
    nan_value = float("NaN")
    empty_dataframe = pd.DataFrame()
    log_file_path = get_log_file()
    if not log_file_path:
        logging.warning(f"There is no file to process")
        return False, empty_dataframe, empty_dataframe
    logging.info(f"Processing {log_file_path} File")
    response = requests.get(log_file_path)
    log_content = io.BytesIO(response.content)
    log_content = io.TextIOWrapper(log_content, encoding='utf-8')
    df = pd.DataFrame(
        columns=[
            'remote_host',
            'request_time',
            'method',
            'request_path_str',
            'request_query_str',
            'status',
            'req_referer_query_str',
            'req_user_agent'])
    try:
        with apache.ApacheSource(log_content,log_format='%h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-Agent}i"') as source:
            for row in source:
                try:
                    row_req_Referer_query_str = row.req_Referer.query_str
                except Exception as e:
                    row_req_Referer_query_str = ""
                new_row = {
                    'remote_host':row.remote_host, 
                    'request_time':row.time, 
                    'method':row.request.method, 
                    'request_path_str':row.request.url.path_str,
                    'request_query_str': row.request.url.query_str,
                    'status': row.status,
                    'req_referer_query_str': row_req_Referer_query_str,
                    'req_user_agent': row.req_User_Agent}
                #append row to the dataframe
                df = df.append(new_row, ignore_index=True)
    except Exception as e:
        logging.error(e)   
        return False, empty_dataframe, empty_dataframe
        
    # Capture dashboard ID
    logging.info(f"Size of dataframe {df.shape}")
    df = df[df["request_path_str"].str.contains('dashboard', case=False)]
    logging.info(f"Size of dataframe after filtering dashboard {df.shape}")

    df["dashboard_id"] = df["request_path_str"].str.split(pat="dashboard/").str[1].str.split(pat="/").str[0]
    df["len_request_query_str"] = df["request_query_str"].str.len()
    df = df.sort_values(by=['remote_host', 'request_time', 'len_request_query_str'])
    df.drop_duplicates(subset=['remote_host', 'request_time'], keep='last', inplace=True)
    df['request_query_str']= df.request_query_str.apply(decode_url)
    df['req_referer_query_str']= df.req_referer_query_str.apply(decode_url)
    df["id"] = df["remote_host"].astype(str) + df["request_time"].astype(str)
    df["id"] = df["id"].str.replace('.', '', regex=True)
    df["id"] = df["id"].str.replace('-', '', regex=True)
    df["id"] = df["id"].str.replace(':', '', regex=True)
    df["id"] = df["id"].str.replace(' ', '', regex=True)
    df["remote_host"] = df["remote_host"].astype(str)
    df_log_dashboard_events = df[["id", "remote_host", "request_time", "dashboard_id", "method", "request_path_str", "request_query_str", "status", "req_referer_query_str", "req_user_agent"]]
    df_dashboard_event_filter = df[["id",  "request_query_str"]]
    df_dashboard_event_filter = df_dashboard_event_filter.rename(
        columns={
            "id" : "request_id"
        })
    df_dashboard_event_filter["request_query_str"] = df_dashboard_event_filter["request_query_str"].str.strip()
    df_dashboard_event_filter["request_query_str"] = np.where(
        (
            (df_dashboard_event_filter['request_query_str'] == "") 
            |  ~(df_dashboard_event_filter['request_query_str'].str.contains('parameters', case=False))
        ), 
        "[]",  df_dashboard_event_filter["request_query_str"])
    df_dashboard_event_filter["request_query_str"] = df_dashboard_event_filter["request_query_str"].str.replace('parameters=', '', regex=True)
    df_dashboard_event_filter["request_query_str"] = df_dashboard_event_filter["request_query_str"].apply(json.loads)

    df_dashboard_event_filter["district"] = ""
    df_dashboard_event_filter["block"] = ""

    for i in df_dashboard_event_filter.index:
        for item in df_dashboard_event_filter.at[i,'request_query_str']:
            if "block" in item["target"][1]:
                if isinstance(item["value"], list):
                    df_dashboard_event_filter.at[i,'block'] = item["value"][0]
                else:
                    df_dashboard_event_filter.at[i,'block'] = item["value"]

            if "District" in item["target"][1]:
                if isinstance(item["value"], list):
                    df_dashboard_event_filter.at[i,'district'] = item["value"][0]
                else:
                    df_dashboard_event_filter.at[i,'district'] = item["value"]
    df_dashboard_event_filter = df_dashboard_event_filter[['request_id', 'district', 'block']]
    return True, df_log_dashboard_events,  df_dashboard_event_filter 
