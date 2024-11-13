# Process-Web-Log

# INFO-H420: Management of Data Science and Business Workflows

## Assignment 3: Workflows with Apache Airflow

### Overview
This assignment involves developing an Apache Airflow workflow (`DAG`) that processes a web server log file. The workflow performs data extraction, transformation, and loading (ETL) tasks, along with a notification step. The main goal is to analyze the log file, filter out specific entries, and archive the results.

### Objectives
- **Define the DAG**: Create a DAG named `process_web_log` that runs daily.
- **Task 1**: Scan for the log file (`scan_for_log`) in the folder `the_logs` and trigger the workflow if `log.txt` is found.
- **Task 2**: Extract IP addresses from the log file and save them into `extracted_data.txt`.
- **Task 3**: Transform the extracted data by filtering out IP `198.46.149.143` and saving the output to `transformed_data.txt`.
- **Task 4**: Load the transformed data into a tar file named `weblog.tar`.
- **Task 5**: Add an additional task to send a notification upon workflow completion.

### Files Included
- `process_web_log.py1-2-3`: The Python script containing the Airflow DAGs solutions for the three team members.
- `README.md`: This documentation file.

