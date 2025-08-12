from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.email import send_email
import pandas as pd
from datetime import datetime

curr_date = {{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%Y-%m-%d) }}
year = curr_date.strftime('%Y')
month = curr_date.strftime('%m')
day = curr_date.strftime('%d')


def check_ehr_file():
    """
    Check ehr.csv for NULL in patient_id, valid datetime in admission_date, and number of records > 0
    """
    curr_file = f's3://healthcare-analytics-datalake/staging/ehr/{year}/{month}/{day}/ehr_{curr_date}.csv'
    df = pd.read_csv(curr_file)

    num_rows = df.shape[0]
    if num_rows == 0:
        raise_exception = 1
        error_msg = "No record detected in EHR data."

    patient_id_null_count = df['patient_id'].isnull().sum()
    if patient_id_null_count > 0:
        raise_exception = 1
        error_msg = "Null detected in patient_id: {patient_id_null_count}. "

    df['admission_date_converted'] = pd.to_datetime(df['admission_date'], errors='coerce')
    admission_date_invalid_count = df['admission_date_converted'].isna().sum()
    if admission_date_invalid_count > 0:
        raise_exception = 1
        error_msg = error_msg + "Invalid datetime detected in admission_date: {admission_date_invalid_count}."

    if raise_exception == 1:
        task_fail_msg = "Errors detected in EHR data, data will not be copied from staging to raw. " + error_msg
        raise AirflowException(task_fail_msg)


def dag_failure_alert(context):
    dag_run: DagRun = context.get('dag_run')
    execution_date = context.get('execution_date')
    failed_task_instances = dag_run.get_task_instances(state='failed') 
    
    message = f"DAG: {dag_run.dag_id} failed on {execution_date}.\n" \
              f"Failed tasks: {[ti.task_id for ti in failed_task_instances]}"

    send_email(
        to = ['your_email@example.com'],
        subject = f"Airflow DAG Failure: {dag_run.dag_id}",
        html_content = f"<h3>{message}</h3>"
    )


with DAG(
    dag_id = 'healthcare_analytics_dag',
    start_date = datetime(2025, 8, 1),
    schedule_interval = '@daily',
    catchup = False,
    on_failure_callback = dag_failure_alert,
    tags = ['S3', 'S3KeySensor', 'S3CopyObjectOperator', 'S3DeleteObjectsOperator'],
) as dag:

    # Check file in source bucket
    sense_file_task = S3KeySensor(
        task_id = 'sense_source_file',
        bucket_name = 's3://healthcare-analytics-source/',
        bucket_key = f'ehr/ehr_{curr_date}.csv',
        aws_conn_id = 'aws_default',
        poke_interval = 60,
        timeout = 600,      # Check file every minute for 10 minutes
        soft_fail = False,
    )

    # Delete same file from staging
    delete_file_from_staging_task = S3DeleteObjectsOperator(
        task_id = 'delete_file_from_staging',
        bucket = 's3://healthcare-analytics-datalake/',
        keys = f'staging/ehr/{year}/{month}/{day}/ehr_{curr_date}.csv',
        aws_conn_id = 'aws_default',
    )

    # Copy file to staging
    copy_file_to_staging_task = S3CopyObjectOperator(
        task_id = 'copy_file_to_staging',
        source_bucket_name = 's3://healthcare-analytics-source/',
        source_bucket_key = f'staging/ehr_{curr_date}.csv',
        dest_bucket_name = 's3://healthcare-analytics-datalake/',
        dest_bucket_key = f'staging/ehr/{year}/{month}/{day}/ehr_{curr_date}.csv',
        aws_conn_id = 'aws_default',
    )

    check_ehr_task = PythonOperator(
        task_id = 'check_ehr_data',
        python_callable = check_ehr_file,
        retries=0,
    )

    # Delete same file from datalake
    delete_file_from_raw_task = S3DeleteObjectsOperator(
        task_id = 'delete_file_from_raw',
        bucket = 's3://healthcare-analytics-datalake/',
        keys = f'staging/ehr/{year}/{month}/{day}/ehr_{curr_date}.csv',
        aws_conn_id = 'aws_default',
    )

    # Copy file to datalake
    copy_file_to_raw_task = S3CopyObjectOperator(
        task_id = 'copy_file_to_raw',
        source_bucket_name = 's3://healthcare-analytics-datalake/',
        source_bucket_key = f'staging/ehr/{year}/{month}/{day}/ehr_{curr_date}.csv',
        dest_bucket_name = 's3://healthcare-analytics-datalake/',
        dest_bucket_key = f'raw/ehr/{year}/{month}/{day}/ehr_{curr_date}.csv',
        aws_conn_id = 'aws_default',
    )

    # Define task dependency
    sense_file_task >> delete_file_from_staging_task >> copy_file_to_staging_task >> check_ehr_task >> delete_file_from_raw_task >> copy_file_to_raw_task