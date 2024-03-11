from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os

os.environ['HADOOP_CONF_DIR'] = '/mnt/c/Users/Satriyo Wisnu/hadoop-3.3.6/etc/hadoop/'

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="spark_analysis_dag",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="DAG to run Spark analysis job on 'retail' table",
    start_date=days_ago(1),
)

submit_spark_job = SparkSubmitOperator(
    task_id="submit_spark_job",
    application="/spark-scripts/spark_scripts.py",
    conn_id="spark_default",  # Make sure you have configured the Spark connection properly
    jars="/mnt/c/Users/Satriyo Wisnu/.m2/repository/org/postgresql/postgresql/42.2.18/postgresql-42.2.18.jar",  # Provide the path to the PostgreSQL JDBC driver JAR
    dag=dag,
)

submit_spark_job