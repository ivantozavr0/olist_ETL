from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date, col
import uuid
from datetime import datetime
from common.spark_init import spark
from common.logger import get_logger

logger = get_logger('log.log', 'bronze_logs')


bucket_name = 'olist'

### параметры запуска
BATCH_TIMESTAMP = datetime.now() 
BATCH_ID = BATCH_TIMESTAMP.strftime("%Y%m%d%H%M%S") 

logger.info(f'@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Заупск Bronze layer Job в {BATCH_TIMESTAMP.strftime("%Y-%m-%d %H:%M:%S")} @@@@@@@@@@@@@@@@@@@@@@@@@@@@')

datasets = [
    "olist_orders_dataset",
    "olist_customers_dataset",
    "olist_order_payments_dataset",
    "olist_order_items_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "olist_geolocation_dataset",
    "olist_order_reviews_dataset"
]

def process_dataset(dataset_name: str, spark):

    logger.info(f"Обрабатываем файл {dataset_name}")

    input_path = f"s3a://{bucket_name}/raw/{dataset_name}.csv"
    output_path = f"s3a://{bucket_name}/bronze/{dataset_name.replace('_dataset', '')}"

    ##### чтение
    df = spark.read.csv(input_path, header=True, inferSchema=False)

    ##### добавляем колонки,которые указывают на время загрузки 
    df = df.withColumn("ingestion_timestamp", lit(BATCH_TIMESTAMP)) \
           .withColumn("ingestion_date", to_date(col("ingestion_timestamp"))) \
           .withColumn("batch_id", lit(BATCH_ID)) \
           .withColumn("source_file", lit(dataset_name))

    #### запись в Parquet с партиционированием 
    (df.write
     .mode("append")
     .partitionBy("ingestion_date", "batch_id")
     .parquet(output_path))

    logger.info(f"{dataset_name} загружен в bronze (batch_id={BATCH_ID})")
    print(f"{dataset_name} загружен в bronze (batch_id={BATCH_ID})")

#### запуск для всех датасетов
for ds in datasets:
    process_dataset(ds, spark)
    
    
    
    
    
    
    
    

