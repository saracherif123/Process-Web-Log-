
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from datetime import datetime
import os
import tarfile

FILE_PATH = "/tmp/the_logs/log.txt"
EXTRACTED_FILE_PATH = "/tmp/the_logs/extracted_data.txt" 
TRANSFORMED_FILE_PATH = "/tmp/the_logs/transformed_data.txt"
TAR_FILE_PATH = "/tmp/the_logs/weblog.tar"

CHANEL = "#first-slack"

def scan_for_log(ti):
    if os.path.exists(FILE_PATH):
        with open(FILE_PATH, "r") as file:
            content = file.read()
            ti.xcom_push(key='log', value=content)
    else:
        print("File not found:", FILE_PATH)

def extract_data(ti):
    file = ti.xcom_pull(task_ids='scan_for_log_task', key='log')
    n = 15  # Number of characters you want to grab from each part
    list = [part[-n:].strip('\n" ') for part in file.split("- -")]
    
    # Use a common path for the extracted file
    with open(EXTRACTED_FILE_PATH, "w") as new_file:
        for item in list:
            new_file.write(f"{item}\n")
    
    
    if os.path.exists(EXTRACTED_FILE_PATH) and os.path.getsize(EXTRACTED_FILE_PATH) > 0:
        print(f"File '{EXTRACTED_FILE_PATH}' was written successfully.")
    else:
        print(f"Failed to write to '{EXTRACTED_FILE_PATH}'.")

def transform_data():
    if os.path.exists(EXTRACTED_FILE_PATH):
        with open(EXTRACTED_FILE_PATH, "r") as file:
            content = file.readlines()
            print(f"File was read successfully: {content}")
        
        for line in content:
            if line.strip() == '198.46.149.143':
                continue
            else:
                with open(TRANSFORMED_FILE_PATH, "w") as new_file:
                    new_file.write(f"{line.strip()}\n")
    
    else:
        print("File not found:", EXTRACTED_FILE_PATH)
        
    if os.path.exists(TRANSFORMED_FILE_PATH) and os.path.getsize(TRANSFORMED_FILE_PATH) > 0:
        print(f"File '{TRANSFORMED_FILE_PATH}' was written successfully.")
    else:
        print(f"Failed to write to '{TRANSFORMED_FILE_PATH}'.")

def load_data():
    if os.path.exists(TRANSFORMED_FILE_PATH):
       with tarfile.open(TAR_FILE_PATH, 'w') as tar:
            tar.add(TRANSFORMED_FILE_PATH, arcname=TRANSFORMED_FILE_PATH.split("/")[3])
    else:
        print("File not found:", TRANSFORMED_FILE_PATH)
    
    if os.path.exists(TAR_FILE_PATH) and os.path.getsize(TAR_FILE_PATH) > 0:
        print(f"File '{TAR_FILE_PATH}' was written successfully.")
    else:
        print(f"Failed to write to '{TAR_FILE_PATH}'.")



# Define the DAG
with DAG(
    'process_web_log',
    description ='Assignment 3 - INFO-H420 - Management of Data Science and Business Workflows',
    schedule ='@daily',
    start_date = datetime(2023, 1, 1),
    catchup=False,) as dag:
    


    # Task to read the file
    scan_for_log_task = PythonOperator(
        task_id='scan_for_log_task',
        python_callable=scan_for_log
    )
    
    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data
    )
    
    transform_data_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data
    )
    
    load_data_task= PythonOperator(
        task_id='load_data_task',
        python_callable=load_data
    )
    
    slack_notification_task = SlackAPIPostOperator(
        task_id="salck_notification_task",
        dag=dag,
        text="The workflow was executed successfully.",
        channel="#first-slack",
    )



    scan_for_log_task >> extract_data_task >> transform_data_task >> load_data_task >> slack_notification_task