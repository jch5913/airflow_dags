from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


with DAG(
    dag_id = 'master_dag',
    start_date = datetime(2025, 8, 1),
    schedule_interval = '@daily',
    catchup = False,
    tags = ['orchestrator'],
) as dag:
    # Task to trigger 'watch_events_dag'
    trigger_watch_events = TriggerDagRunOperator(
        task_id = 'trigger_watch_events_dag',
        trigger_dag_id = 'watch_events_dag',
        wait_for_completion = True,
        poke_interval = 5,
    )

    # Task to trigger 'metadata_dag'
    trigger_metadata = TriggerDagRunOperator(
        task_id = 'trigger_metadata_dag',
        trigger_dag_id = 'metadata_dag',
        wait_for_completion = True,
        poke_interval = 5,
    )

    # Task to trigger 'users_dag'
    trigger_users = TriggerDagRunOperator(
        task_id = 'trigger_users_dag',
        trigger_dag_id = 'users_dag',
        wait_for_completion = True,
        poke_interval = 5,
    )

    # Task to run 'calculate_engagement_score'
    calculate_engagement_score_task = PythonOperator(
        task_id = 'calculate_engagement_score',
        python_callable = calculate_engagement_score,
    )

    # Define task dependencies
    [trigger_watch_events, trigger_metadata, trigger_users] >> run_final_calculation