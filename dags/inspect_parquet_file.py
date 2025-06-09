from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import os

# Correct paths to the partition directories
POLLUTION_PARQUET_PATH = '/home/ccd/airflow/data/formatted/combined/istanbul/2025-06-05/'
WEATHER_PARQUET_PATH = '/home/ccd/airflow/data/formatted/combined/istanbul/2025-06-05/'

def inspect_static_parquet():
    spark = SparkSession.builder \
        .appName("StaticParquetInspection") \
        .master("local[*]") \
        .getOrCreate()
    if os.path.exists(POLLUTION_PARQUET_PATH):
        print("=== Pollution Parquet Schema ===")
        pollution_df = spark.read.parquet(POLLUTION_PARQUET_PATH)
        pollution_df.printSchema()
        pollution_df.show(truncate=False, n=20)
    else:
        print(f"[ERROR] Pollution file not found: {POLLUTION_PARQUET_PATH}")

    if os.path.exists(WEATHER_PARQUET_PATH):
        print("=== Weather Parquet Schema ===")
        weather_df = spark.read.parquet(WEATHER_PARQUET_PATH)
        weather_df.printSchema()
        weather_df.show(truncate=False, n=20)
    else:
        print(f"[ERROR] Weather file not found: {WEATHER_PARQUET_PATH}")

    spark.stop()

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='inspect_static_parquet_file_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 5, 30),
    catchup=False,
    tags=['debug', 'inspect', 'static'],
) as dag:

    inspect_task = PythonOperator(
        task_id='inspect_static_parquet',
        python_callable=inspect_static_parquet
    )
