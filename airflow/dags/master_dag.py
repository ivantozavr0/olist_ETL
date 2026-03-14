from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta
import os

### конфигурация
SPARK_MASTER = os.getenv('AIRFLOW_CONN_SPARK_DEFAULT', 'spark://spark-master:7077')
SPARK_BINARY = os.getenv('SPARK_BINARY', "/opt/spark/bin/spark-submit") 
CONN_ID = os.getenv('CONN_ID', "spark_default")
BASE_JOBS_PATH = os.getenv('JOBS_PATH')

### скачивание
DOWNLOAD_SCRIPT = os.path.join(BASE_JOBS_PATH, "download", "download.py")

#### бронзовый слой
BRONZE_SCRIPT = os.path.join(BASE_JOBS_PATH, "bronze_jobs", "bronze_job.py")

#### silver слой
SILVER_SCRIPTS = [
    os.path.join(BASE_JOBS_PATH, "silver_jobs", f)
    for f in ["sellers.py", "customers.py","geolocation.py", "items.py",
              "orders.py", "payments.py", "reviews.py", "products.py"]
] 

CLICKHOUSE_SCRIPT = os.path.join(BASE_JOBS_PATH, "create_star", "star.py")


# параметры Spark 
DEFAULT_SPARK_CONF = {
    "spark.master": SPARK_MASTER,
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}

SPARK_SUBMIT_BASE = dict(
    spark_binary=SPARK_BINARY,
    conn_id=CONN_ID,
    conf=DEFAULT_SPARK_CONF,
    executor_cores=2,
    executor_memory="2g",
    driver_memory="2g",
)

#### общие параметры для всех операторов
DEFAULT_ARGS = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

###### построение DAG
with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["etl", "spark"],
    description="Полный ETL-процесс: бронза → серебро → витрины",
) as dag:

    ##### 0. Скачивание
    bash_task = BashOperator(
        task_id='download_olist_script',
        bash_command=f'python {DOWNLOAD_SCRIPT}',
    )

    ####### 1. бронзовый слой 
    bronze_task = SparkSubmitOperator(
        task_id="bronze_layer",
        application=BRONZE_SCRIPT,
        **SPARK_SUBMIT_BASE
    )


    #### 2. серебряный слой — группа из 8 задач

    with TaskGroup(group_id="silver_layer") as silver_group:
        silver_tasks = []
        for idx, script_path in enumerate(SILVER_SCRIPTS):
            script_name = os.path.basename(script_path).replace(".py", "")
            task = SparkSubmitOperator(
                task_id=f"silver_{script_name}",
                application=script_path,
                **SPARK_SUBMIT_BASE
            )
            silver_tasks.append(task)


    #### 3. заполнение кликхауса
    clickhouse_task = SparkSubmitOperator(
        task_id="clickhouse_filling",
        application=CLICKHOUSE_SCRIPT,
        **SPARK_SUBMIT_BASE
    )


    bash_task >> bronze_task >> silver_group >> clickhouse_task


