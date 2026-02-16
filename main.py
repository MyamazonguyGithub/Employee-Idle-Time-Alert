import os
import json
import calendar
import requests
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import logging
from collections import defaultdict
from rate_limiter.python.airtable_throttler import AirtableThrottler
from pyairtable.api import Api as AirtableApi
from rate_limiter.python.package_throttler import PackageThrottler
from rate_limiter.python.time_doctor_throttler import TimeDoctorThrottler

from slack_sdk import WebClient 
from slack_sdk.errors import SlackApiError

load_dotenv()
time_doctor_throttler = TimeDoctorThrottler()
airtable_throttle = PackageThrottler((), max_operations_in_window=5, rate_limit_window=1).execute_with_throttle


def get_table():
    print("Retrieving workers from Airtable")
    try:
        airtable = AirtableApi(os.getenv("AIRTABLE_API_KEY"))
        table = airtable.table("appfccXiah8EtMfbZ", "Workers")
        records = airtable_throttle(table, 'all', view="Active Workers")
    except Exception as e:
        logging.error(f"Error retrieving Airtable records: {e}")
    return records

def search_workers(email):
    print(f"Searching for worker with email: {email}")
    params = {
        "company": "YFpYQwOkUAAEWZlH",
        "filter[email]": email
    }

    url = "https://api2.timedoctor.com/api/1.0/users"
    resp = time_doctor_throttler.throttled_get(url, params=params)
    resp_json = resp.json()
    data = resp_json['data']
    try:
        lastSeen = data[0]['lastSeen'].get('updatedAt', '')
        lastSeenStr = lastSeen.split("T")[0]
        hireDate = data[0].get('hiredAt',"").split("T")[0]
        return { 
            'id': data[0]['id'],
            'lastSeen': lastSeenStr,
            'hireDate': hireDate
        }
    except Exception as e:
        logging.error(f"Error while searching worker: {e}")
        return False
    
def sendSlackAlert(data):
    rows = [[
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_section",
                    "elements": [
                        {
                            "type": "text",
                            "text": "Employee",
                            "style": {
                                "bold": True
                            }
                        }
                    ]
                }
            ]
        },
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_section",
                    "elements": [
                        {
                            "type": "text",
                            "text": "Idle Time %",
                            "style": {
                                "bold": True
                            }
                        }
                    ]
                }
            ]
        },
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_section",
                    "elements": [
                        {
                            "type": "text",
                            "text": "Total Time Worked",
                            "style": {
                                "bold": True
                            }
                        }
                    ]
                }
            ]
        }
    ]]
    def employee_cell(d): 
        # If we have a valid Slack ID, use a real mention element 
        slack_id = d.get("slack_id") 
        if isinstance(slack_id, str) and slack_id.startswith(("U", "W")): 
            return { "type": "rich_text", "elements": [ { "type": "rich_text_section", "elements": [ {"type": "user", "user_id": slack_id} ] } ] } 
        # Fallback to plain name 
        return { "type": "rich_text", "elements": [ { "type": "rich_text_section", "elements": [ {"type": "text", "text": d["name"]} ] } ] }
    
    for d in data:
        rows.append([
            employee_cell(d),
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {
                                "type": "text",
                                "text": d['idle_time'],
                                "style": {
                                    "bold": False
                                }
                            }
                        ]
                    }
                ]
            },
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {
                                "type": "text",
                                "text": d['total_time_str'],
                                "style": {
                                    "bold": False
                                }
                            }
                        ]
                    }
                ]
            }
        ])
    client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

    try:
        if data[0]['manager_slack_id'] is None:
            logging.error(f"No Slack ID found in Airtable for manager: {data[0]['manager']}. Cannot send Slack alert.")
            return
        
        response = client.chat_postMessage(
            channel= os.getenv("TEST_SLACK_CHANNEL_ID") if dev_mode and os.getenv("TEST_SLACK_CHANNEL_ID")!="" else data[0]['manager_slack_id'],
            text="Employee Idle-Time Alert: See details below.",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "plain_text",
                        "text": f"__Test: to be sent to {data[0]['manager']}__ \n\n\n:warning: Employee Idle-Time Alert \n\n Pay Period: {period} \n",
                        "emoji": True
                    }
                },
                {
                    "type": "table",
                    "column_settings": [
                        {
                            "is_wrapped": True
                        },
                        {
                            "align": "right"
                        }
                    ],
                    "rows": rows
                },
                {
                    "type": "section",
                    "text": {
                        "type": "plain_text",
                        "text": "The following employee(s) have exceeded the idle-time threshold. Please review and take appropriate action.",
                        "emoji": True
                    }
                }
            ]
        )
        print("Message sent:", response["ts"])
    except SlackApiError as e:
        print(f"Error sending message: {e.response['error']}")


def search_worker_stats(td_user_id, today, start_date):
    try:
        today -=timedelta(days=1)  # Adjust 'to' date to be yesterday
        detail = "summary-ratio"
        params = {
            "company": "YFpYQwOkUAAEWZlH",
            "user": td_user_id,
            "from": f'{start_date.strftime("%Y-%m-%d")}T00:00:00Z',
            "to": f'{today.strftime("%Y-%m-%d")}T23:59:59Z'
        }
        url = f"https://api2.timedoctor.com/api/1.0/stats/{detail}"
        resp = time_doctor_throttler.throttled_get(url, params=params)
        resp_json = resp.json()
        data = resp_json['data']
        user = data['users'][0]
        idle_percent = f'{user["idleMinsRatio"]*100:.2f}%'
        total_hours_worked = round(user['total']/3600, 2)
        hours = int(total_hours_worked)
        minutes = f'{(total_hours_worked % 1) * 60:.0f}'
        total_hours_worked_str = f"{hours}h {minutes}m"
        return {
            'idle_float': user["idleMinsRatio"]*100,
            'idle_percent': idle_percent,
            'total_hours_worked': total_hours_worked,
            'total_hours_worked_str': total_hours_worked_str
        }
    except Exception as e:
        logging.error(f"Error while retrieving worker stats: {e}")
        return {
            'idle_float': 0,
            'idle_percent': "-",
            'total_hours_worked': "-",
            'total_hours_worked_str': "-"
        }

def openJsonFile(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return {}
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {file_path}")
        return {}

def main(dev_mode=False):
    print(" --------------- Starting execution")
    if dev_mode: 
        print("Running in development mode")
        today = datetime.strptime(os.getenv("TEST_DATE_TODAY"), "%Y-%m-%d").date()
    else: 
        today = datetime.today().date()

    if today.weekday() == 0: # Monday - Check if today is payday
        ref_date_str = os.getenv("PAYDAY_REFERENCE_DATE")
        ref_date = datetime.strptime(ref_date_str, "%Y-%m-%d").date()
        delta_days = (today - ref_date).days
        is_payday = (delta_days % 14 == 0)
        start_date  = today - timedelta(days=14)
        if not is_payday:
            return
    if today.weekday() == 6: # Sunday *2. Google Sheet Auto-Population (Weekly)* every week
        start_date  = today - timedelta(days=6)
        
    global period
    period =start_date.strftime("%b %d, %Y") + " – " + (today - timedelta(days=1)).strftime("%b %d, %Y")

    data = []
    try:
        workers= get_table()
        for i, worker in enumerate(workers):

            currentPositionLevel = worker['fields'].get('Current Position Level', [])[0]
            if currentPositionLevel in ['Director', 'Executive/VP', 'President']:
                continue

            #dev mode filters
            if dev_mode and os.getenv("TEST_USER") !='' and worker['fields'].get('Worker') != os.getenv("TEST_USER"): continue
            if dev_mode and os.getenv("TEST_SAMPLE_SIZE") !='':
                if i >= int(os.getenv("TEST_SAMPLE_SIZE")):
                    break

            iter_data = {}
            name = worker['fields'].get('Worker')
            email = worker['fields'].get('Work Email Address')[0]
            slack_id = worker['fields'].get('Slack Member ID')
            
            position = worker['fields'].get('Current Position Title')
            position_title = position[0] if position else None

            director_list = worker['fields'].get("Director Name") or [] 
            director = director_list[0] if director_list else None

            manager_list = worker['fields'].get("Manager Name") or [] 
            manager = manager_list[0] if manager_list else director
            
            director_slack_list = worker['fields'].get("Director Slack Member ID") or [] 
            director_slack_id = director_slack_list[0] if director_slack_list else None

            manager_slack_list = worker['fields'].get("Manager Slack Member ID") or [] 
            manager_slack_id = manager_slack_list[0] if manager_slack_list else director_slack_id
            iter_data = {
                'slack_id': slack_id,
                'name': name,
                'email': email,
                'position_title': position_title,
                'manager': manager,
                'director': director,
                'manager_slack_id': manager_slack_id,
                'total_time': "-",
                'total_time_str': "-",
                'idle_float': 0,
                'idle_time': "-"
            }

            print(f"Processing worker ({i+1}/{len(workers)}): {worker['fields'].get('Worker', 'Unknown')}")
            
            if not worker['fields'].get('Work Email Address'):
                continue

            td_user = search_workers(email)
            if not td_user:
                iter_data['name'] += " (TD User Not Found)" 
                data.append(iter_data)
                logging.warning(f"Time Doctor user not found for email: {email}")
                continue
            
            td_worker_stats = search_worker_stats(td_user['id'], today, start_date)
            iter_data['total_time'] = td_worker_stats['total_hours_worked']
            iter_data['total_time_str'] = td_worker_stats['total_hours_worked_str']
            iter_data['idle_float'] = td_worker_stats['idle_float']
            iter_data['idle_time'] = td_worker_stats['idle_percent']
            data.append(iter_data)

        if today.weekday() == 0: # Monday - Check if today is payday
            managers = list({d['manager'] for d in data})
            idle_time_map = openJsonFile("idle-time-per-role.json")
            for mgr in managers:
                filtered_data = []
                for d in data:
                    if d['manager']==mgr:
                        if d['idle_float'] > idle_time_map.get(d['position_title'], 15):
                            filtered_data.append(d)
                if filtered_data:
                    sendSlackAlert(filtered_data)
        if today.weekday() == 6: # Sunday *2. Google Sheet Auto-Population (Weekly)* every week
            pass
    except Exception as e:
        logging.error(f"An error occurred: {e}")

import time
if __name__ == "__main__":
    global dev_mode
    dev_mode = os.getenv("TEST_MODE", "FALSE").lower() == "true"

    start = time.time()
    main(dev_mode)
    end = time.time()
    print(f"--------------- Execution Completed in {(end - start)/60} minutes")