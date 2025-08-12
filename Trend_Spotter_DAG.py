from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator
from datetime import datetime

with DAG(
    dag_id = 'trend_spotter_dag',
    start_date = datetime(2025, 8, 1),
    schedule_interval = '0 3/1 * * *',   # Runs hourly from 03:00 UTC to 23:00
    catchup = False,
    tags = ['S3', 'S3KeySensor', 'S3CopyObjectOperator', 'S3DeleteObjectsOperator'],
) as dag:

    curr_date = {{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%Y-%m-%d) }}
    year = curr_date.strftime('%Y')
    month = curr_date.strftime('%m')
    day = curr_date.strftime('%d')

    # Check file in source bucket
    sense_file_task = S3KeySensor(
        task_id = 'sense_source_file',
        bucket_name = 's3://trend-spotter-source/',
        bucket_key = f'sales-extracts/sales_{curr_date}.csv',
        aws_conn_id = 'aws_default',
        poke_interval = 60,
        timeout = 600,      # Check file every minute for 10 minutes
        soft_fail = False,
    )

    # Delete file from datalake with same key
    delete_file_task = S3DeleteObjectsOperator(
        task_id = 'delete_file_from_datalake',
        bucket = 's3://trend-spotter-datalake/',
        keys = f'raw/sales/{year}/{month}/{day}/sales_{curr_date}.csv',
        aws_conn_id = 'aws_default',
    )

    # Copy file to datalake
    copy_file_task = S3CopyObjectOperator(
        task_id = 'copy_file_to_datalake',
        source_bucket_name = 's3://trend-spotter-source/',
        source_bucket_key = f'sales-extracts/sales_{curr_date}.csv',
        dest_bucket_name = 's3://trend-spotter-datalake/',
        dest_bucket_key = f'raw/sales/{year}/{month}/{day}/sales_{curr_date}.csv',
        aws_conn_id = 'aws_default',
    )

    # Define task dependency
    sense_file_task >> delete_file_task >> copy_file_task