from pyspark.sql.functions import col, lit, countDistinct, to_date, desc, count

from pyspark.sql import functions as F

from silver_jobs.shared import *
from common.spark_init import spark

from common.logger import get_logger

logger = get_logger('geolocation_log.log', 'silver_logs')

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

PROCESS_NAME = "geolocation"
    
#### приведение типов
def casting(df):
    
    double_caster = StandardTypeCaster(spark, bucket_name, quarantine_layer, logger, "double")
    double_columns = [
        "geolocation_lat",
        'geolocation_lng'
    ]
    df_clean = double_caster.cast(df, double_columns, "{PROCESS_NAME}_bad")
    
    return df_clean

    
#### приводим города и коды штатов к унифицированной записи
#### проверяем валидность координат
def buisiness_validation(df):

    df = df.withColumn("geolocation_city", F.lower("geolocation_city"))
    df = df.withColumn("geolocation_state", F.upper("geolocation_state"))

    df = df.filter((col('geolocation_lng') >= -180) & (col('geolocation_lng') <= 180))
    df = df.filter((col('geolocation_lat') >= -90) & (col('geolocation_lat') <= 90))
    
    return df
    

##### усредним координаты по ключу ["geolocation_zip_code_prefix", 'geolocation_city', 'geolocation_state']
### это кардинально уменьшит фрейм, но мы не потеряем информацию о координатах с приемлемой точностью
def avg_coords(df):
    df_geo = df.groupBy(["geolocation_zip_code_prefix", 'geolocation_city', 'geolocation_state']) \
            .agg(
                F.avg("geolocation_lat").alias("geolocation_lat"),
                F.avg("geolocation_lng").alias("geolocation_lng")
            )
            
    return df_geo
    
    
###### записываем в серебряный слой
def write_silver(df_final, name, logger):
    df_final = df_final.select(
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state",
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
    
    df_final = avg_coords(df_clean)
    
    final_count = df_final.count()
    logger.info(f"Final silver rows in {PROCESS_NAME}: {final_count}")
    logger.info(f"Rows dropped in {PROCESS_NAME}: {initial_count - final_count}")
    
    write_silver(df_final, PROCESS_NAME, logger)
 


if __name__ == "__main__":
    run_etl()













