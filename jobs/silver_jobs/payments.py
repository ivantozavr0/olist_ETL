from pyspark.sql.functions import col, lit, countDistinct, to_date, desc, count

from pyspark.sql import functions as F

from silver_jobs.shared import *
from common.spark_init import spark
from common.logger import get_logger

logger = get_logger('payments_log.log', 'silver_logs')

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
    int_caster = StandardTypeCaster(spark, bucket_name, quarantine_layer, logger, "int")
    int_columns = [
        "payment_sequential",
        "payment_installments",
    ]
    df_clean = int_caster.cast(df, int_columns, "payments_bad")
    
    double_caster = StandardTypeCaster(spark, bucket_name, quarantine_layer, logger, "double")
    double_columns = [
        "payment_value",
    ]
    df_clean = double_caster.cast(df_clean, double_columns, "payments_bad")
    
    return df_clean

    
### цены положительные.
#### payment_installments - кол-во способов оплаты => как минимум, одно
def buisiness_validation(df):

    df = df.filter(F.col("payment_value") > 0.)
    df = df.filter(F.col("payment_installments") >= 1)
    
    return df
    
###### записываем в серебряный слой
def write_silver(df_final, name, logger):
    df_final = df_final.select(
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
    "ingestion_timestamp"
    )

    df_final.write \
        .mode("overwrite") \
        .parquet(f"s3a://{bucket_name}/{silver_layer}/{name}")
        
    logger.info(f"Записал {name}")


def run_etl():

    logger.info(f'@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Заупск Silver layer Job для Payments @@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    
    df, initial_count = read_dataset(spark, 'olist_order_payments', logger)
    
    df_clean = casting(df)
    
    df_clean = buisiness_validation(df_clean)
    
    df_final = deduplication(df_clean, ["order_id", "payment_sequential"])
    
    final_count = df_final.count()
    logger.info(f"Final silver rows in Payments: {final_count}")
    logger.info(f"Rows dropped in Payments: {initial_count - final_count}")
    
    write_silver(df_final, 'payments', logger)
 


if __name__ == "__main__":
    run_etl()













