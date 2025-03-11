from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from config_loader import config
from logger import logger

mongo_client = MongoClient(config["MONGODB"]["uri"])
db = mongo_client[config["MONGODB"]["database"]]
collection = db[config["MONGODB"]["collection"]]

consumer = KafkaConsumer(
    config["KAFKA_LOCAL"]["topic"],
    bootstrap_servers=config["KAFKA_LOCAL"]["bootstrap_servers"],
    security_protocol=config["KAFKA_LOCAL"]["security_protocol"],
    sasl_mechanism=config["KAFKA_LOCAL"]["sasl_mechanism"],
    sasl_plain_username=config["KAFKA_LOCAL"]["sasl_username"],
    sasl_plain_password=config["KAFKA_LOCAL"]["sasl_password"],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='local-consumer-group',
    auto_offset_reset='earliest'
)

consumer.subscribe([config["KAFKA_LOCAL"]["topic"]])
logger.info(f"Subscribed to Kafka Local topic: {config['KAFKA_LOCAL']['topic']}")

try:
    for msg in consumer:
        collection.insert_one(msg.value)
        logger.info(f"Saved to MongoDB: {msg.value}")
        print(f"Saved to MongoDB: {msg.value}")
except Exception as e:
    logger.error("Error:", exc_info=e)
finally:
    consumer.close()
