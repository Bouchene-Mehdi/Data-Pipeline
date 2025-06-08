import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import os
def test_spark():
    print("JAVA_HOME:", os.environ.get("JAVA_HOME"))

    spark = SparkSession.builder \
        .appName("AirflowSparkTest") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.createDataFrame([(1, "OK"), (2, "Spark Works")], ["id", "message"])
    df.show()

    spark.stop()

with DAG(
    dag_id='test_spark_connection',
    start_date=datetime(2025, 5, 30),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=["test", "spark"],
) as dag:

    test_spark_task = PythonOperator(
        task_id='test_spark_session',
        python_callable=test_spark
    )
