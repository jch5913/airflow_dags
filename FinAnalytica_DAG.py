from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator, BranchPythonOperator
import pandas as pd
import os
from datetime import datetime


curr_date = {{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%Y-%m-%d) }}
year = curr_date.strftime('%Y')
month = curr_date.strftime('%m')
day = curr_date.strftime('%d')

def download_parse_source_file():
    """
    Download and parse source file, this runs only on first day of month
    """
    s3_hook = S3Hook(aws_conn_id = 'aws_default')

    bucket_name_source = 's3://finanalytica-source/',
    bucket_key_source = f'historical-stocks/stocks_{year}_{month}.csv.gz',

    local_path_source = f'/source_file/stocks_{year}_{month}.csv.gz'

    s3_hook.download_file(key = bucket_key_source, bucket_name = bucket_name_source, local_path = local_path_source)
    df = pd.read_csv(local_path_source, compression='gzip')

    # Make daily data file based on unique dates in df
    column_to_split = 'Date'
    unique_dates = df[column_to_split].unique()

    for dt in unique_dates:
        df_curr = df[df[column_to_split] == dt]
        dt = dt.strftime('%Y-%m-%d)
        output_filename = f'/output_file/stock_data_{dt}.csv'
        df_curr.to_csv(output_filename, index = False)    

    # Clean up local source file after daily files are made
    os.remove(local_path_source)


def upload_daily_file():
    """
    Upload file to S3, this runs daily
    """
    local_file = f'/output_file/stock_data_{curr_date}.csv'

    bucket_name_target = 's3://finanalytica-datalake/',
    bucket_key_target = f'/raw/stocks/{year}/{month}/{day}/stock_data_{curr_date}.csv',

    s3_hook = S3Hook(aws_conn_id = 'aws_default')

    # Overwrite if the key exists, no need to remove key first
    s3_hook.load_file(key = bucket_key_target, bucket_name = bucket_name_target, filename = local_file, replace = True)

    # Clean up local daily file after upload
    os.remove(local_file)


def choose_path():
    """
    On first day of a month, first download and parse source data, then upload daily data
    On other days of a month, only upload daily data
    """
    if day == 1:
        return 'download_parse_s3_data'
    else:
        return 'upload_data_to_s3'


with DAG(
    dag_id = 'finanalytica_dag',
    start_date = datetime(2020, 1, 1),
    schedule_interval = '@daily',
    catchup = False,
    tags = ['S3', 'pandas', 'S3Hook'],
) as dag:

    branch_task = BranchPythonOperator(
        task_id = 'branch_decision',
        python_callable = choose_path,
        provide_context = True,
    )

    download_parse_s3_data_task = PythonOperator(
        task_id = 'download_parse_s3_data',
        python_callable = download_parse_source_file,
    )

    upload_data_to_s3_task = PythonOperator(
        task_id = 'upload_data_to_s3',
        python_callable = upload_daily_file,
    )

    # On first day of a month, run download and parse, then upload
    branch_task >> download_parse_s3_data_task
    download_parse_s3_data_task >> upload_data_to_s3_task

    # On other days of a month, run upload only
    branch_task >> upload_data_to_s3_task
