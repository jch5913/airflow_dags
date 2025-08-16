from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime


curr_date = {{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%Y-%m-%d) }}
year = curr_date.strftime('%Y')
month = curr_date.strftime('%m')
day = curr_date.strftime('%d')

firm_aws_conn = 'adclick_aws'

# Define Glue Crawler config
adclick_clickstream_crawler_config = {
    'Name': 'adclick_clickstream_crawler',
    'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',   # Need to create this Glue IAM Role ARN
    'DatabaseName': 'adclick_clickstream_database',
    'Targets': {
        'S3Targets': [
            {
                'Path': 's3://adclick-datalake/raw/clickstream/',
            }
        ]
    }
}


with DAG(
    dag_id = 'trend_spotter_dag',
    start_date = datetime(2025, 8, 1),
    schedule_interval = '@daily',
    catchup = False,
    tags = ['S3', 'S3CopyObjectOperator', 'Glue', 'Crawler', 'GlueCrawlerOperator'],
) as dag:

    # Delete data from datalake with the same key
    delete_data_task = S3DeleteObjectsOperator(
        task_id = 'delete_data_from_datalake',
        bucket = 's3://adclick-datalake/',
        keys = f'raw/clickstream/{year}/{month}/{day}/clickstream_{curr_date}.json',
        aws_conn_id = firm_aws_conn,
    )

    # Move data to datalake
    move_data_task = S3CopyObjectOperator(
        task_id = 'move_data_to_datalake',
        source_bucket_name = 's3://adclick-source/',
        source_bucket_key = f'clickstream/clickstream_{curr_date}.json',
        dest_bucket_name = 's3://adclick-datalake/',
        dest_bucket_key = f'raw/clickstream/{year}/{month}/{day}/clickstream_{curr_date}.json',
        aws_conn_id = firm_aws_conn,
    )

    # Trigger the crawler
    trigger_crawler_task = GlueCrawlerOperator(
        task_id = 'trigger_glue_crawler',
        config = adclick_clickstream_crawler_config,
        aws_conn_id = firm_aws_conn,
    )

    # Define task dependency
    delete_data_task >> move_data_task >> trigger_crawler_task