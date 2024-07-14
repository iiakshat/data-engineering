import json
import requests
from datetime import datetime
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner' : 'akshat',
    'start_date' : datetime(2024, 7, 13, 10, 00)
}

def get_data():
    
    res = requests.get('https://randomuser.me/api').json()
    return res['results'][0]


def format_data(data):

    op = {}
    location = data['location']
    op['first_name'] = data['name']['first']
    op['last_name'] = data['name']['last']
    op['gender'] = data['gender']
    op['address'] = f'{location["street"]}, {location["city"]}, {location["state"]}, {location["country"]}'
    op['email'] = data['email']
    op['postcode'] = location['postcode']
    op['dob'] = data['dob']['date']
    op['phone'] = data['phone']
    op['date_registered'] = data['registered']['date']
    op['picture'] = data['picture']['medium']

    return op

def stream_data():

    data = get_data()
    data = format_data(data)
    print(json.dumps(data, indent = 4))

# with DAG('urer_automation',
#         default_args = default_args,
#         schedule_interval = '@daily',
#         catchup = False) as dag:
    
#     task = PythonOperator(
#         task_id = 'stream_data',
#         python_callable = stream_data
#     )