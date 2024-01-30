import requests
from datetime import datetime
import uuid
import time
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    'owner': 'agung',
    'start_date': datetime.today()
}

def get_data():
    url = 'https://randomuser.me/api/'

    response = requests.get(url)
    data = response.json()

    return data['results'][0]

def format_data(data):
    fmt_data = {}
    location = data['location']

    fmt_data['id'] = uuid.uuid4()
    fmt_data['first_name'] = data['name']['first']
    fmt_data['last_name'] = data['name']['last']
    data['gender'] = data['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = data['email']
    data['username'] = data['login']['username']
    data['dob'] = data['dob']['date']
    data['registered_date'] = data['registered']['date']
    data['phone'] = data['phone']
    data['picture'] = data['picture']['medium']

    return data

def stream_data():
    curr_time = time.time()

    producer = KafkaProducer(bootstrap_servers=['broker:29092'],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'),
                             max_block_ms=60000)

    while True:
        # set break condition after 1 minute
        if time.time() - curr_time > 60:
            break

        try:
            data = get_data()
            formatted_data = format_data(data)
            # print(formatted_data)

            producer.send('user_data', formatted_data)
            logging.info(f"Data sent...")
        except Exception as e:
            logging.error(e)
            continue

    producer.flush()
    producer.close()

with DAG(dag_id='user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='streaming_api',
        python_callable=stream_data
    )
        