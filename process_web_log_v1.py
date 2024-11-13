from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os
import requests  # Import requests for sending HTTP requests

# Define the base path
base_path = '/usr/local/airflow/dags'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='A simple workflow to process web server logs',
    schedule_interval=timedelta(days=1),
)

# Task 1: Scan for log
# Defines a FileSensor task that scans for the presence of log.txt at specified intervals (poke_interval).
#  It times out if the file isn't found within 300 seconds.
scan_for_log = FileSensor(
    task_id='scan_for_log',
    filepath=os.path.join(base_path, 'log.txt'),
    poke_interval=60,
    timeout=300,
    dag=dag,    # not using contenxt manager makes us need to set it in every task
)

# Task 2: Extract data
#A Python function that reads log.txt, extracts the first word (assumed to be an IP address) from each line, and writes it to extracted_data.txt
def extract_ip_addresses():
    with open(os.path.join(base_path, 'log.txt'), 'r') as log_file:
        with open(os.path.join(base_path, 'extracted_data.txt'), 'w') as output_file:
            for line in log_file:
                ip_address = line.split()[0]
                output_file.write(ip_address + '\n')

# A PythonOperator that executes the extract_ip_addresses function as a task within the DAG.
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_ip_addresses,
    dag=dag,
)

# Task 3: Transform data
#A function that reads extracted_data.txt, filters out lines containing the IP 198.46.149.143,
# and writes the remaining data to transformed_data.txt
def transform_data():
    with open(os.path.join(base_path, 'extracted_data.txt'), 'r') as input_file:
        with open(os.path.join(base_path, 'transformed_data.txt'), 'w') as output_file:
            for line in input_file:
                if line.strip() != '198.46.149.143':
                    output_file.write(line)

# A PythonOperator that runs the transform_data function as a task.
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Task 4: Load data
# A function that compresses transformed_data.txt into a tar file named weblog.tar.
def load_data():
    import tarfile
    with tarfile.open(os.path.join(base_path, 'weblog.tar'), "w") as tar:
        tar.add(os.path.join(base_path, 'transformed_data.txt'), arcname='transformed_data.txt')

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Task 5: Send notification to Slack
# A function that sends a notification to Slack using an incoming webhook URL. 
# It checks the response status and raises an error if the request fails.
def send_slack_notification(**kwargs):
    webhook_url = "Add your webhook link here" 
    message = {
        "text": "The workflow has been executed successfully!"
    }
    
    response = requests.post(webhook_url, json=message)
    
    if response.status_code != 200:
        raise ValueError(f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}")

notify_slack = PythonOperator(
    task_id='notify_slack',
    python_callable=send_slack_notification,
    provide_context=True,
    dag=dag,
)

# Define the task sequence
scan_for_log >> extract_data >> transform_data_task >> load_data_task >> notify_slack