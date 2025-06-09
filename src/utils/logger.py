import logging
from pathlib import Path

def get_logger(name):
    """
    Configure and return a logger.
    name: Logger name
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    Path("data/logs").mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler("data/logs/app.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    return logger