from pyspark.sql.functions import col, lit, countDistinct, to_date, desc, count

from pyspark.sql import functions as F

from silver_jobs.shared import *
from common.spark_init import spark
#from logger import logger

from common.logger import get_logger

logger = get_logger('customers_log.log', 'silver_logs')

"""
Общая схема (может немного видоизменяться от таблицы к таблице):
1) Чтение из bronze_layer
2) Приведение типов
3) Бизнес валидация и модификация (отсеиваем нереалистичные значения)
4) Дедупликация (bronze_layer партицируется по дате и батчу загрузки, + могут быть просто дубликаты в изначальном массиве)
5) Запись в silver_layer 
"""


bucket_name = "olist"
bronze_layer = "bronze"
silver_layer = "silver"
quarantine_layer = "quarantine"
    
    
#### приводим города и коды штатов к унифицированной записи
def buisiness_validation(df):

    df = df.withColumn("customer_city", F.lower("customer_city"))
    df = df.withColumn("customer_state", F.upper("customer_state"))
    
    return df
    
###### записываем в серебряный слой
def write_silver(df_final, name, logger):
    df_final = df_final.select(
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
    "ingestion_timestamp"
    )

    df_final.write \
        .mode("overwrite") \
        .parquet(f"s3a://{bucket_name}/{silver_layer}/{name}")
        
    logger.info(f"Записал Customers")


def run_etl():

    logger.info(f'@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Заупск Silver layer Job для Customers @@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    
    df, initial_count = read_dataset(spark, 'olist_customers', logger)
    
    df_clean = buisiness_validation(df)
    
    df_final = deduplication(df_clean, 'customer_id')
    
    final_count = df_final.count()
    logger.info(f"Final silver rows in Customers: {final_count}")
    logger.info(f"Rows dropped in Customers: {initial_count - final_count}")
    
    write_silver(df_final, 'customers', logger)
 


if __name__ == "__main__":
    run_etl()













