from pyspark.sql.functions import col, lit, countDistinct, to_date, desc, count

from pyspark.sql import functions as F

from silver_jobs.shared import *
from common.spark_init import spark
from common.logger import get_logger

logger = get_logger('items_log.log', 'silver_logs')

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

PROCESS_NAME = "items"
    
    
#### приведение типов
def casting(df):
    
    int_caster = StandardTypeCaster(spark, bucket_name, quarantine_layer, logger, "int",
                        write_mode='overwrite')
    int_columns = [
        "order_item_id",
    ]
    df = int_caster.cast(df, int_columns, "{PROCESS_NAME}_bad")
    
    
    ts_caster = TimestampCaster(spark, bucket_name, quarantine_layer, logger, "yyyy-MM-dd HH:mm:ss",
                    write_mode='append')
    timestamp_columns = [
        "shipping_limit_date",
    ]
    df = ts_caster.cast(df, timestamp_columns, "{PROCESS_NAME}_bad")
    
    
    double_caster = StandardTypeCaster(spark, bucket_name, quarantine_layer, logger, "double",
                        write_mode='append')
    double_columns = [
        "price",
        "freight_value"
    ]
    df = double_caster.cast(df, double_columns, "{PROCESS_NAME}_bad")
    
    return df

    
### должны быть критически важные order_id и product_id
#### цена не должна быть отрицательной
def buisiness_validation(df):

    df = df.filter(col('order_id').isNotNull())
    df = df.filter(col('product_id').isNotNull())
    
    df = df.filter(col('price') > 0.)
    df = df.filter(col('freight_value') >= 0.)
    
    return df
    
    
###### записываем в серебряный слой
def write_silver(df_final, name, logger):
    df_final = df_final.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "ingestion_timestamp"
    )

    df_final.write \
        .mode("overwrite") \
        .parquet(f"s3a://{bucket_name}/{silver_layer}/{name}")
        
    logger.info(f"Записал {name}")


def run_etl():

    logger.info(f'@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Заупск Silver layer Job для {PROCESS_NAME.upper()} @@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    
    df, initial_count = read_dataset(spark, f'olist_order_{PROCESS_NAME}', logger)
    
    df_clean = casting(df)
    
    df_clean = buisiness_validation(df_clean)
    
    df_final = deduplication(df_clean, ["order_id", "order_item_id"])
    
    final_count = df_final.count()
    logger.info(f"Final silver rows in {PROCESS_NAME}: {final_count}")
    logger.info(f"Rows dropped in {PROCESS_NAME}: {initial_count - final_count}")
    
    write_silver(df_final, PROCESS_NAME, logger)
 


if __name__ == "__main__":
    run_etl()













