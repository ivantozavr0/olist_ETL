from pyspark.sql import SparkSession
import os

minio_login = os.getenv("MINIO_ROOT_USER")
minio_pass = os.getenv("MINIO_ROOT_PASSWORD")

spark = (SparkSession.builder
        .appName("Silver jobs")
        .master("spark://spark-master:7077")
        .config("spark.jars", "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/clickhouse-jdbc-0.8.4-all.jar") 
        # Настройки для MinIO (S3A)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", minio_login) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_pass) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    )
