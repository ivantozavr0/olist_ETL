from pyspark.sql import functions as F
from pathlib import Path
import os
from datetime import datetime

from common.spark_init import spark
from common.clickhouse_client import get_client, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
from common.logger import get_logger


bucket_name = "olist"
silver_layer = "silver"
TARGET_DB = "olist"


logger = get_logger('star_log.log', 'star_logs')


#### задаем структуру базы данных
def init_database(name):

    client = get_client(os.getenv("CLICKHOUSE_DB"))
    client.command(f"create database if not exists {name}")
    client.close()
    
    client = get_client(name)
    
    #### инициализация звездной структуры
    sql_path = Path("/opt/spark/clickhouse-schema/star-schema")

    for file in sql_path.glob("*.sql"):
        with open(file) as f:
            client.command(f.read())
    
    #### инициализация материализованных представлений
    sql_path = Path("/opt/spark/clickhouse-schema/views")       
    for file in sql_path.glob("*.sql"):
        with open(file) as f:
            client.command(f.read())
            
    client.close()

def read_parquet(name, spark):
    df = spark.read.parquet(f"s3a://{bucket_name}/{silver_layer}/{name}")
    return df


def read_datasets(datasets, spark):
    
    dfs = []
    
    for dataset in datasets:
        df = read_parquet(dataset, spark)
        dfs.append(df)
    
    
    return dfs
    

def make_customer_dim(df_customers):
    dim_customer = (
            df_customers
            .select("customer_unique_id")
            .dropDuplicates()
        )
        
    return dim_customer
    
def make_product_dim(df_products):
    dim_product = (
            df_products
            .select(
                "product_id",
                "product_category_name"
            )
            .withColumnRenamed(
                "product_category_name",
                "category"
            )
        )
        
    return dim_product
        
        
def make_seller_dim(df_sellers):
    dim_seller = (
            df_sellers
            .select(
                "seller_id",
                "seller_city",
                "seller_state"
            )
        )
        
    return dim_seller
    
    
def make_location_dim(df_customers, df_sellers):
    dim_location = (
            df_customers.select(F.col("customer_city").alias('city'), 
                                F.col("customer_state").alias('state'))
            .union(
                df_sellers.select(F.col("seller_city").alias('city'), 
                                F.col("seller_state").alias('state'))
            )
            .distinct()
        )
        
    return dim_location
    
    
def make_delivery_dim(df_orders):
    dim_delivery = (
            df_orders
            .filter(F.col("order_status") == "delivered")
            .select(
                "order_id",
                'order_purchase_timestamp',
                "order_delivered_customer_date",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_estimated_delivery_date",
                'is_delivery_date_missing',
            )
        )
        
    dim_delivery = (
            dim_delivery
            .withColumn(
                "delivery_days",
                F.datediff(
                    "order_delivered_customer_date",
                    "order_purchase_timestamp"
                )
            )
        )
        
    return dim_delivery
    

def calc_payments_per_order(df_payments):
    payments_per_order = (
        df_payments
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("payment_value")
        )
    )
        
    return payments_per_order
    
    
def prepare_reviews(df_reviews):
    reviews = (
        df_reviews
        .groupBy(
            "order_id",
        )
        .agg(F.avg("review_score").alias('review_score'))
    )
    
    return reviews
    
    
def make_orders_enriched(df_orders, df_customers):
    orders_enriched = (
            df_orders
            .join(
                df_customers.select(
                    "customer_id",
                    "customer_unique_id",
                    "customer_city",
                    "customer_state"
                ),
                "customer_id"
            )
        )
        
    return orders_enriched
            
            
            
def make_fact_order_items(df_items, orders_enriched, df_payments, df_reviews):

    fact_order_items = (
            df_items
            .join(orders_enriched, "order_id")
            .select(
                "order_id",
                "order_item_id",
                
                "order_purchase_timestamp",
                
                "order_status",
                
                "customer_unique_id",
                "customer_city",
                "customer_state",

                "product_id",
                "seller_id",

                "price",
                "freight_value",
            )
        )     
        
    return fact_order_items      
    
    
def make_fact_orders(orders_enriched, df_payments, df_reviews):

    payments_per_order = calc_payments_per_order(df_payments)
    reviews = prepare_reviews(df_reviews)

    fact_orders = (
        orders_enriched
        .join(payments_per_order, "order_id", "left")
        .join(reviews, "order_id", "left")
        .select(
            "order_id",
            
            "order_purchase_timestamp",
            
            "customer_unique_id",
            "customer_city",
            "customer_state",
             
            "order_status",
                
            "payment_value",
            "review_score"
        )
    )

    return fact_orders
        
    
def write_to_click(df, table, db_name):
    (
        df.write
        .format("jdbc")
        .mode("append")
        .option("url", f"jdbc:clickhouse://clickhouse:8123/{db_name}")
        .option("dbtable", table)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("batchsize", "100000")
        .save()
    )
    
    
def fill_star(table_df_list, db_name):

    client = get_client(db_name)

    for table_df in table_df_list:
        table, df = table_df
        
        client.command(f"truncate {table}")
        client.command(f"truncate {table}")  
            
        write_to_click(df, table, db_name)
        
            
def main():
    global logger, spark, TARGET_DB
    
    logger.info(f"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Старт {datetime.now()}@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")   
    
    init_database(TARGET_DB) 
    logger.info(f"БД инициализирована")
    
    datasets = ['orders', 'payments', 'items', 'customers', 
                    'products', 'sellers', 'reviews']
        
    (df_orders, df_payments, df_items, 
        df_customers, df_products, df_sellers, df_reviews) = read_datasets(datasets, spark)    
        
    logger.info(f"Прочитал parquet")
   
    ### готовим таблицы измерений
    
    df_customer_dim = make_customer_dim(df_customers)
    
    df_product_dim = make_product_dim(df_products)
    
    df_seller_dim = make_seller_dim(df_sellers)
    
    df_delivery_dim = make_delivery_dim(df_orders)
    
    df_location_dim = make_location_dim(df_customers, df_sellers)
    
    logger.info(f"Измерения подготовлены")   
    
    
    ### плоские таблицы фактов (факт - покупка одного конкретного товара).
    
    #### не будем создавать отдельное измерение для покупателя, 
    ### так как никакой уникальной информации о нем на самом деле нет
    ### city и state - не уникальны для одного уникального пользователя (customer_unique_id).
    ### то есть customer_state и customer_city на самом деле относятся к конкретному заказу, а не покупателю.
    #### в рамках модели предлагается убрать customer_id, как ненужную прослойку
    #### между заказом и рельным пользователем.
    #### обе таблицы фактов денормализованы по покупателю (в них ключены данные о месте заказа). 
    
    orders_enriched = make_orders_enriched(df_orders, df_customers)
    
    df_fact_order_items = make_fact_order_items(df_items, orders_enriched, df_payments, df_reviews)  
    df_fact_orders =  make_fact_orders(orders_enriched, df_payments, df_reviews)  
    
    logger.info(f"Таблицы фактов подготовлены") 
    
    #### запись в базу
    table_df_list = [("dim_customer", df_customer_dim), ("dim_product", df_product_dim),
                    ("dim_seller", df_seller_dim), ("dim_location", df_location_dim),
                    ("dim_delivery", df_delivery_dim),
                    ("fact_order_items", df_fact_order_items),
                    ("fact_orders", df_fact_orders)
                    ]
    
    fill_star(table_df_list, TARGET_DB)
    
    logger.info(f"Записал в ClickHouse") 
            
            
if __name__ == '__main__':
    main()   
            
            
            
            
            
            
