from pyspark.sql.functions import col, lit, countDistinct, to_date, desc, count

from pyspark.sql import functions as F

from silver_jobs.shared import *
from common.spark_init import spark
from common.logger import get_logger

logger = get_logger('products_log.log', 'silver_logs')

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

PROCESS_NAME = "products"
    
#### приведение типов
def casting(df):
    
    int_caster = StandardTypeCaster(spark, bucket_name, quarantine_layer, logger, "int",
                        write_mode='overwrite')
    int_columns = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]
    df = int_caster.cast(df, int_columns, "{PROCESS_NAME}_bad")
    
    return df

    
##### заполняем пропущенные категории как unknown
def buisiness_validation(df):

    df = df.fillna({"product_category_name": "unknown"})
    
    return df
    
    
###### записываем в серебряный слой
def write_silver(df_final, name, logger):
    df_final = df_final.select(
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "ingestion_timestamp"
    )

    df_final.write \
        .mode("overwrite") \
        .parquet(f"s3a://{bucket_name}/{silver_layer}/{name}")
        
    logger.info(f"Записал {name}")


def run_etl():

    logger.info(f'@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Заупск Silver layer Job для {PROCESS_NAME.upper()} @@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    
    df, initial_count = read_dataset(spark, f'olist_{PROCESS_NAME}', logger)
    
    df_clean = casting(df)
    
    df_clean = buisiness_validation(df_clean)
    
    df_final = deduplication(df_clean, ["product_id"])
    
    final_count = df_final.count()
    logger.info(f"Final silver rows in {PROCESS_NAME}: {final_count}")
    logger.info(f"Rows dropped in {PROCESS_NAME}: {initial_count - final_count}")
    
    write_silver(df_final, PROCESS_NAME, logger)
 


if __name__ == "__main__":
    run_etl()













