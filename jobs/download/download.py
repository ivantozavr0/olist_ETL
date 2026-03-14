import s3fs
import os

import kagglehub

from common.logger import get_logger

logger = get_logger('log.log', 'download_logs')

bucket_name = os.getenv("MINIO_BUCKET")
raw_layer = os.getenv("RAW_LAYER")
minio_login = os.getenv("MINIO_ROOT_USER")
minio_pass = os.getenv("MINIO_ROOT_PASSWORD")
DATA_DIR = os.getenv("DATA_DIR")

datasets = [f"{f}.csv" for f in[
    "olist_orders_dataset",
    "olist_customers_dataset",
    "olist_order_payments_dataset",
    "olist_order_items_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "olist_geolocation_dataset",
    "olist_order_reviews_dataset"
]]


def check_files(fs):
    logger.info(f"Проверка наличия файлов")
    files = set([f.split('/')[-1] for f in fs.ls(f'{bucket_name}/{raw_layer}/')])
    logger.info(f"{files}")
    for dataset in datasets:
        if dataset not in files:
            return False
            
    return True


def download():
    down_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    logger.info(f"Датасет скачан с Kaggle")
    return down_path

def get_fs():
    fs = s3fs.S3FileSystem(
        key=minio_login,
        secret=minio_pass,
        client_kwargs={'endpoint_url': 'http://minio:9000'}
    )

    return fs

def move_files(path, fs):
    logger.info(f"Начинаем перемещение файлов")
    for file in os.listdir(path):
        if file.endswith('.csv'):
            old_path = os.path.join(path, file)
            dest_path = f'{bucket_name}/{raw_layer}/{file}'
            with open(old_path, 'rb') as csv_file:
                with fs.open(dest_path, 'wb') as dest_file:
                    dest_file.write(csv_file.read())
                    
    logger.info(f"Файлы {os.listdir(path)} успешно перемещены")

def main():
    logger.info(f'@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Запуск Download Job @@@@@@@@@@@@@@@@@@@@@@@@@@@@')

    fs = get_fs()
    
    if not fs.exists(bucket_name):
        fs.mkdir(bucket_name)
    
    if not check_files(fs):
    
        logger.info(f"Не все файлы")
        
        down_path = download()

        move_files(down_path, fs)   
    else:
        logger.info(f"Все файлы есть")   
    
    logger.info(f'@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Успех @@@@@@@@@@@@@@@@@@@@@@@@@@@@')    
                

if __name__ == '__main__':
    main()      
                
                
                
                
                
                
                
                
                
