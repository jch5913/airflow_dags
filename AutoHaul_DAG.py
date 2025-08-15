from airflow import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator, SFTPOperation
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime


with DAG(
    dag_id = 'process_trip_data',
    schedule_interval = '@daily',
    start_date = datetime(2025, 8, 1),
    catchup = False,
    tags = ['S3', 'S3CreateObjectOperator', 'GlueJobOperator'],
) as dag:

    curr_date = {{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%Y-%m-%d) }}
    year = curr_date.strftime('%Y')
    month = curr_date.strftime('%m')
    day = curr_date.strftime('%d')

    download_from_sftp_task = SFTPOperator(
        task_id = 'download_trip_data',
        ssh_conn_id = 'autohaul_sftp',     # This is the pre configured sftp connection in Airflow UI Admin->Connections
        remote_filepath = f'extract/trip_data_{curr_date}.csv',
        local_filepath = f'extract/trip_data_{curr_date}.csv',
        operation = SFTPOperation.GET,
    )

    upload_to_s3_task = S3CreateObjectOperator(
        task_id = "upload_trip_data",
        s3_bucket = 's3://autohaul-datalake/',
        s3_key = f'trip_data/raw/{year}/{month}/{day}/trip_data_{curr_date}.csv',
        data = f'extract/trip_data_{curr_date}.csv',
        replace = True,   # Set to True to overwrite if the key exists
        aws_conn_id = 'aws_autohaul_1234',
    )

    # Run the Glue ETL job
    run_glue_job_task = GlueJobOperator(
        task_id = "run_glue_job",
        job_name = "autohaul-trip-data-glue-job",
        script_location = "s3://autohaul-datalake/trip_data/scripts/trip_data_glue_script.py",
        s3_bucket = "s3://autohaul-datalake/",
        iam_role_name = "autohaul-glue-job-role",
        retry_limit = 0,
    )


    # Define task dependency
    download_from_sftp_task >> upload_to_s3_task >> run_glue_job_task
