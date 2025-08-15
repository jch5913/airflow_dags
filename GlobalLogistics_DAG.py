from airflow import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator, SFTPOperation
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import os
from datetime import datetime


curr_date = {{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%Y-%m-%d) }}
year = curr_date.strftime('%Y')
month = curr_date.strftime('%m')
day = curr_date.strftime('%d')

firm_aws_conn = 'globallogistics_aws'


def upload_track_file(dc_id):
    """
    Upload file to S3
    """
    local_file = f'/exports/{dc_id}/track.csv'

    bucket_name_target = 's3://globallogistics-datalake/',
    bucket_key_target = f'/raw/exports/{dc_id}/track_{dc_id}.csv',

    s3_hook = S3Hook(aws_conn_id = firm_aws_conn)

    # Overwrite if the key exists, no need to remove key first
    s3_hook.load_file(key = bucket_key_target, bucket_name = bucket_name_target, filename = local_file, replace = True)

    # Clean up local daily file after upload
    os.remove(local_file)


def create_dynamic_dag(dc_id, sftp_conn_id, source_path):
    """
    Creates a dynamic DAG that moves a csv file from an sftp to s3
    """
    with DAG(
        dag_id = f'ingest_dc_{dc_id}',
        schedule_interval = '@daily',
        start_date = datetime(2025, 8, 1),
        catchup = False,
        tags = [f'dynamic_dag_{dc_id}'],
    ) as dag:

        download_file = SFTPOperator(
            task_id = f'download_{dc_id}_track_file',
            ssh_conn_id = sftp_conn_id,         # Need to pre configure this in Airflow UI Admin->Connections
            remote_filepath = source_path,
            local_filepath = f'/exports/{dc_id}/track.csv',
            operation = SFTPOperation.GET,
        )

        upload_data_to_s3_task = PythonOperator(
            task_id = f'upload_{dc_id}_track_file_to_s3',
            python_callable = upload_track_file(dc_id),
        )

        # Define task dependency
        download_file >> upload_data_to_s3_task

    return dag


# Read the dcs_config.json into a dict
with open('config/dcs_config.json', 'r') as file:
    dcs_config = json.load(file)

# Generate multiple DAGs using the factory function
for config in dcs_config:
    globals()[f'dynamic_dag_{dcs_config['dcs_id']}'] = create_dynamic_dag(dcs_config['dcs_id'], dcs_config['sftp_conn_id'], dcs_config['source_path'])