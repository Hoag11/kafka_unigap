import logging
from config_loader import config

def setup_logger():
    log_file = config["LOGGING"]["log_file"]
    log_level = getattr(logging, config["LOGGING"]["log_level"].upper(), logging.INFO)

    logging.basicConfig(
        filename=log_file,
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger("KafkaPipeline")

logger = setup_logger()
logger.info("Logger initialized successfully!")
