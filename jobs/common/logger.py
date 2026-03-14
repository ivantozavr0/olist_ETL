import logging
import os
from pathlib import Path

def get_logger(log_file, log_dir=None):

    base_path = Path(os.getenv("JOBS_LOG_PATH"))

    if log_dir:
        base_path = base_path / log_dir
        os.makedirs(base_path, exist_ok=True)
        

    logging.basicConfig(
            filename=base_path / log_file, 
            filemode='a', 
            level=logging.INFO,
            encoding='utf-8',
            format="%(asctime)s %(levelname)s %(message)s"
        )
        
    logger = logging.getLogger(__name__)
    
    return logger
