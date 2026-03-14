from pyspark.sql.functions import col, lit, countDistinct, to_date, desc, count, isnull, when, median, mean, to_timestamp

from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F
from pyspark.sql import Window

from silver_jobs.shared import *
from common.spark_init import spark
from common.logger import get_logger

logger = get_logger('orders_log.log', 'silver_logs')


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

#### приведение типов
def casting(df):
    caster = TimestampCaster(spark, bucket_name, quarantine_layer, logger, "yyyy-MM-dd HH:mm:ss")
    timestamp_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    df = caster.cast(df, timestamp_columns, "orders_bad_timestamps")
    
    return df
    
    
def buisiness_validation(df_clean):

    #### должен быть id у заказа и пользователя
    df_clean = df_clean.filter(
    col("order_id").isNotNull() &
    col("customer_id").isNotNull()
)

    ##### order_delivered_customer_date >= order_purchase_timestamp, или заказ вообще не доставлен
    df_clean = df_clean.filter(
        col("order_delivered_customer_date").isNull() |
        (
            col("order_delivered_customer_date") >=
            col("order_purchase_timestamp")
        )
    )

    ############ у доставленных должно быть налчиие даты доставки
    df_clean = df_clean.withColumn(
        "is_delivery_date_missing",
        (col("order_status") == "delivered") &
        (col("order_delivered_customer_date").isNull())
    )

    df_clean.filter(col("is_delivery_date_missing") == True).count()
    
    return df_clean
   
    
###### записываем в серебряный слой
def write_silver(df_final, logger):
    df_final = df_final.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "is_delivery_date_missing",
    "ingestion_timestamp"
    )

    df_final.write \
        .mode("overwrite") \
        .parquet(f"s3a://{bucket_name}/{silver_layer}/orders")
        
    logger.info(f"Записал orders")


def run_etl():

    logger.info(f'@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Заупск Silver layer Job для Orders @@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    
    ##### чтение партиционированного датасета
    df_orders, initial_count = read_dataset(spark, 'olist_orders', logger)
    
    df_clean = casting(df_orders)
    
    df_clean = buisiness_validation(df_clean)
    
    df_final = deduplication(df_clean, 'order_id')
    
    final_count = df_final.count()
    logger.info(f"Final silver rows in olist_orders: {final_count}")
    logger.info(f"Rows dropped in olist_orders: {initial_count - final_count}")
    
    write_silver(df_final, logger)
 


if __name__ == "__main__":
    run_etl()













