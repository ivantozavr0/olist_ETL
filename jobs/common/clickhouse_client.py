import clickhouse_connect
import os

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

def get_client(db_name):

    client = clickhouse_connect.get_client(host='clickhouse', 
                username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
                database=db_name)
                
    return client
