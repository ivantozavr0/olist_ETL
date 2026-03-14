from pyspark.sql import Window
from pyspark.sql import functions as F, DataFrame, Column
from typing import List, Optional
import logging

class TypeCaster:
    """
    Базовый класс для приведения типов колонок DataFrame.
    Реализует общую логику:
    - создание временных колонок с суффиксом '_parsed'
    - поиск строк, где исходное значение не NULL, а распаршенное NULL (битые данные)
    - сохранение битых строк в карантин (parquet)
    - замена исходных колонок на распаршенные в чистом DataFrame
    """
    def __init__(self, spark, bucket_name: str, quarantine_layer: str, logger: logging.Logger,
            write_mode='append'):
        self.spark = spark
        self.bucket_name = bucket_name
        self.quarantine_layer = quarantine_layer
        self.logger = logger
        self.write_mode = write_mode

    def cast(self, df: DataFrame, columns: List[str], quarantine_filename: str) -> DataFrame:
        # создаём временные колонки с суффиксом '_parsed'
        df_with_parsed = self._create_parsed_columns(df, columns)

        # формируем условие для битых строк
        corrupted_condition = self._build_corrupted_condition(columns)

        # выделяем битые строки
        df_bad = df_with_parsed.filter(corrupted_condition)
        bad_count = df_bad.count()
        self.logger.info(f"Corrupted rows count: {bad_count}")

        # сохраняем битые строки в карантин
        self._save_quarantine(df_bad, quarantine_filename)

        # отфильтровываем чистые строки
        df_clean = df_with_parsed.filter(~corrupted_condition)

        # заменяем исходные колонки на распаршенные
        df_clean = self._replace_columns(df_clean, columns)

        return df_clean

      #### parsed в названия колонок
    def _create_parsed_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        for col_name in columns:
            df = df.withColumn(f"{col_name}_parsed", self._cast_column(F.col(col_name)))
        return df

    ##### приведение
    def _cast_column(self, col_expr: Column) -> Column:

        raise NotImplementedError("Subclasses must implement _cast_column")

    ##### условие для поиска битых записей
    def _build_corrupted_condition(self, columns: List[str]) -> Column:

        condition = None
        for col_name in columns:
            cond = F.col(col_name).isNotNull() & F.col(f"{col_name}_parsed").isNull()
            condition = cond if condition is None else (condition | cond)
        return condition

    ##### сохраняем битые строки в карантин, если они есть
    def _save_quarantine(self, df_bad: DataFrame, filename: str) -> None:
        if df_bad.count() > 0:
            path = f"s3a://{self.bucket_name}/{self.quarantine_layer}/{filename}"
            df_bad.write.mode(self.write_mode).parquet(path)
            self.logger.info(f"Saved corrupted rows to {path}")
        else:
            self.logger.info("No corrupted rows to save.")

    #### заменяем исходные колонки на распаршенные
    def _replace_columns(self, df_clean: DataFrame, columns: List[str]) -> DataFrame:
        for col_name in columns:
            df_clean = df_clean.drop(col_name).withColumnRenamed(f"{col_name}_parsed", col_name)
        return df_clean


##### каст к стандартному типу ('integer', 'double', 'boolean') через cast()
class StandardTypeCaster(TypeCaster):

    def __init__(self, spark, bucket_name: str, quarantine_layer: str, logger: logging.Logger, target_type: str,
            write_mode='append'):
        super().__init__(spark, bucket_name, quarantine_layer, logger, write_mode=write_mode)
        self.target_type = target_type

    def _cast_column(self, col_expr: Column) -> Column:
        return col_expr.cast(self.target_type)
        
##### каст времени
class TimestampCaster(TypeCaster):
    def __init__(self, spark, bucket_name: str, quarantine_layer: str, logger: logging.Logger,
                 timestamp_format: str = "yyyy-MM-dd HH:mm:ss", write_mode='append'):
        super().__init__(spark, bucket_name, quarantine_layer, logger, write_mode=write_mode)
        self.timestamp_format = timestamp_format

    def _cast_column(self, col_expr: Column) -> Column:
        return F.to_timestamp(col_expr, self.timestamp_format)


##### чтение партиционированного датасета
def read_dataset(spark, name, logger, bucket_name = "olist", bronze_layer = "bronze"):

    logger.info("Читаю")
    
    df = spark.read.parquet(
        f"s3a://{bucket_name}/{bronze_layer}/{name}"
    )

    initial_count = df.count()
    logger.info(f"Initial bronze rows in {name}: {initial_count}")
    
    return df, initial_count
    
    
#### убираем дубликаты
#### прежде всего по дате загрузки (ingestion_date) и батчу (batch_id)
#### дубликаты в рамках одной партиции удалятся актоматически
def deduplication(df_clean, unique_col):
    window_spec = Window.partitionBy(unique_col).orderBy(F.col("ingestion_date").desc(), 
                                                      F.col("batch_id").desc())

    df_with_row_number = df_clean.withColumn("row_num", F.row_number().over(window_spec))

    df_final = df_with_row_number.filter(F.col("row_num") == 1).drop("row_num")
    
    return df_final






