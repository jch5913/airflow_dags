from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime

curr_date = {{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%Y-%m-%d) }}
year = curr_date.strftime('%Y')
month = curr_date.strftime('%m')
day = curr_date.strftime('%d')

firm_aws_conn = 'salesforcelite_aws'


def query_postgresql_save_csv():
    """
    Query PostgreSQL using PostgresHook and saves the results to a CSV file
    """
    pg_hook = PostgresHook(postgres_conn_id = 'salesforcelite_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    customers_last_updated_at = Variable.get("customers_last_updated_at", default_var="2020-01-01")
    sql_query = f'SELECT * FROM customers WHERE updated_at > {customers_last_updated_at}'
    cursor.execute(sql_query)
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(data, columns=column_names)
    local_file = f'customers/{curr_date}/customers_data_{curr_date}.csv'
    df = pd.read_csv(local_file)

    cursor.close()
    conn.close()

    # Upload to S3
    bucket_name_target = 's3://salesforcelite-datalake/',
    bucket_key_target = f'raw/customers/{year}/{month}/{day}/customers_data_{curr_date}.csv',

    s3_hook = S3Hook(aws_conn_id = firm_aws_conn)

    # Overwrite if the key exists, no need to remove key first
    s3_hook.load_file(key = bucket_key_target, bucket_name = bucket_name_target, filename = local_file, replace = True)


def update_customers_last_updated_at():
    """
    Read the local file into a df, compute max updated_at, update Airflow UI Admin->Variable
    """
    local_file = f'customers/{curr_date}/customers_data_{curr_date}.csv'
    df = pd.read_csv(local_file)
    last_updated_at = df[updated_at].max()
    Variable.set(key="customers_last_updated_at", value=last_updated_at)


with DAG(
    dag_id = 'salesforcelite_dag',
    start_date = datetime(2025, 8, 1),
    schedule_interval = '@daily',
    catchup = False,
    on_failure_callback = dag_failure_alert,
    tags = ['PostgresHook', 'S3', 'S3Hook'],
) as dag:

    make_data_task = PythonOperator(
        task_id = 'make_customers_data',
        python_callable = query_postgresql_save_csv,
        retries=0,
    )

    update_lastupdate_task = PythonOperator(
        task_id = 'update_lastupdate',
        python_callable = update_customers_last_updated_at,
        retries=0,
    )

    # Define task dependency
    make_data_task >> update_lastupdate_task