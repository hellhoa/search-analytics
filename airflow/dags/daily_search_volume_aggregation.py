from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'daily_search_volume_aggregation',
    default_args=default_args,
    description='Aggregate hourly search volume data to daily',
    schedule_interval='1 * * * *',  # Run at minute 1 of every hour
    catchup=False,
    max_active_runs=1
)

aggregate_search_volume = SparkSubmitOperator(
    task_id='aggregate_search_volume',
    application='/opt/airflow/dags/spark/daily_aggregation.py',
    conf={
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
        'spark.jars.packages': 'mysql:mysql-connector-java:8.0.28'
    },
    verbose=True,
    dag=dag
)

aggregate_search_volume