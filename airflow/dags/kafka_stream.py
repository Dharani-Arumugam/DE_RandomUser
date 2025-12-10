import logging
import uuid
import json
import time
import requests

from datetime import datetime
from airflow import DAG
from confluent_kafka import Producer
from airflow.providers.standard.operators.python import PythonOperator


default_args = {
    'owner': 'dharani',
    'start_date': datetime(2023, 11, 4),
}


def get_data(max_retries=5, delay=1):
    """Fetch a valid RandomUser record with retry handling."""

    for attempt in range(max_retries):
        try:
            response = requests.get("https://randomuser.me/api/", timeout=5)

            if response.status_code != 200:
                logging.warning(f"Bad API status {response.status_code}")
                time.sleep(delay)
                continue

            data = response.json()

            # Validate structure
            if (
                "results" in data and
                isinstance(data["results"], list) and
                len(data["results"]) > 0
            ):
                return data["results"][0]

            logging.warning(f"No result found (attempt {attempt+1}/{max_retries})")

        except Exception as e:
            logging.warning(f"API error: {e}")

        time.sleep(delay)

    raise RuntimeError("Failed to fetch valid data after retries")


def format_data(results):
    """Format RandomUser API result safely."""

    loc = results.get("location", {})
    street = loc.get("street", {})

    address = (
        f"{street.get('number', '')} {street.get('name', '')}, "
        f"{loc.get('city', '')}, {loc.get('state', '')}, {loc.get('country', '')}"
    )

    return {
        "id": str(uuid.uuid4()),
        "full_name": f"{results.get('name', {}).get('first', '')} "
                     f"{results.get('name', {}).get('last', '')}",
        "gender": results.get("gender", ""),
        "address": address,
        "postcode": str(loc.get("postcode", "")),
        "email": results.get("email", ""),
        "user_name": results.get("login", {}).get("username", ""),
        "dob": results.get("dob", {}).get("date", ""),
        "registered_date": results.get("registered", {}).get("date", ""),
        "phone": results.get("phone", ""),
        "image": results.get("picture", {}).get("medium", "")
    }


def stream_data_to_kafka():
    """Fetch 20 valid records and publish to Kafka."""

    producer = Producer({
        "bootstrap.servers": "broker:29092",
        "acks": "all"
    })

    sent = 0
    target = 20

    while sent < target:
        try:
            raw = get_data()
            formatted = format_data(raw)

            producer.produce(
                topic="users_created",
                value=json.dumps(formatted).encode("utf-8")
            )

            sent += 1
            logging.info(f"Sent {sent}/{target} records")

        except Exception as e:
            logging.error(f"Kafka publish error: {e}")
            time.sleep(1)

    producer.flush()
    logging.info("All 20 records successfully published to Kafka.")


with DAG(
    dag_id="user_automation",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    streaming_task = PythonOperator(
        task_id="streaming_data_from_api",
        python_callable=stream_data_to_kafka,
    )



# import logging
# import uuid
# import json
# import requests
# import traceback
#
# from datetime import datetime
# from airflow import DAG
# from confluent_kafka import Producer
# from airflow.providers.standard.operators.python import PythonOperator
#
# default_args ={
#     'owner' : 'dharani',
#     'start_date' : datetime( 2023, 11 , 4, 0),
# }
#
#
# def get_data():
#     """Fetch data from RandomUser API"""
#     response      = requests.get('https://randomuser.me/api/')
#     response_json = response.json()
#     print('hello')
#     print(response_json['results'][0])
#     results       = response_json['results'][0]
#     return results
#
# def format_data(results):
#     """Format data for Kafka"""
#     data = {}
#     data['id']              = str(uuid.uuid4())
#     data['name']            = results['name']['first'] + ' ' + results['name']['last']
#     data['gender']          = results['gender']
#     location                = results['location']
#     data['address']         = (  str(location['street']['number'])+ ' '
#                              + location['street']['name'] + ', '
#                              + location['city']+ ', '
#                              + location['state']+ ', '
#                              + location['country'])
#     data['postcode']        = location['postcode']
#     data['email']           = results['email']
#     data['user_name']       = results['login']['username']
#     data['dob']             = results['dob']['date']
#     data['registered_date'] = results['registered']['date']
#     data['phone']           = results['phone']
#     data['image']           = results['picture']['medium']
#
#     #print(data)
#     return data
#
# def stream_data_to_kafka():
#     """Streaming data to Kafka"""
#     # Confluent Kafka Producer configuration
#     config = {
#         "bootstrap.servers": "broker:29092",
#         "linger.ms": 50,
#         "acks": "all"
#     }
#     producer = Producer(config)
#
#     records = 20  # send 20 messages
#
#
#     for _ in range(records):
#         try :
#             print(_)
#             raw_data  = get_data()
#
#             if not raw_data:
#                 logging.info("No data sent")
#                 continue
#             else:
#                 formatted_data = format_data(raw_data)
#                 producer.produce(
#                     topic="users_created",
#                     value=json.dumps(formatted_data).encode('utf-8'))
#         except Exception as e:
#             logging.error(f"Kafka Publish failed with error: {e}")
#             traceback.print_exc()
#
#         #The flush() method in a Kafka producer in Python is used to ensure that
#         # all buffered records are sent to the Kafka broker and acknowledged before the method returns.
#     producer.flush()
#
#
# with DAG(
#     dag_id='user_automation',
#     default_args=default_args,
#     schedule='@daily',
#     catchup=False,
# ) as dag:
#
#     streaming_task = PythonOperator(
#         task_id='streaming_data_from_api',
#         python_callable=stream_data_to_kafka
#     )