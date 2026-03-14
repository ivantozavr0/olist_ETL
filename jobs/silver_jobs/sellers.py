from pyspark.sql.functions import col, lit, countDistinct, to_date, desc, count

from pyspark.sql import functions as F

from silver_jobs.shared import *
from common.spark_init import spark
from common.logger import get_logger

logger = get_logger('sellers_log.log', 'silver_logs')

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

PROCESS_NAME = 'sellers'

    
### города и штаты - к унифицированному стилю
def buisiness_validation(df):

    df = df.withColumn("seller_city", F.lower("seller_city"))
    df = df.withColumn("seller_state", F.upper("seller_state"))
    
    return df
    
###### записываем в серебряный слой
def write_silver(df_final, name, logger):
    df_final = df_final.select(
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
    "ingestion_timestamp"
    )

    df_final.write \
        .mode("overwrite") \
        .parquet(f"s3a://{bucket_name}/{silver_layer}/{name}")
        
    logger.info(f"Записал Payments")


def run_etl():

    logger.info(f'@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Заупск Silver layer Job для {PROCESS_NAME.upper()} @@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    
    df, initial_count = read_dataset(spark, f'olist_{PROCESS_NAME}', logger)
    
    df_clean = buisiness_validation(df)
    
    df_final = deduplication(df_clean, ["seller_id",])
    
    final_count = df_final.count()
    logger.info(f"Final silver rows in {PROCESS_NAME}: {final_count}")
    logger.info(f"Rows dropped in {PROCESS_NAME}: {initial_count - final_count}")
    
    write_silver(df_final, PROCESS_NAME, logger)
 


if __name__ == "__main__":
    run_etl()













