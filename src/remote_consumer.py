from kafka import KafkaConsumer
import json
from config_loader import config
from logger import logger

remote_conf = {
    'bootstrap_servers': config["KAFKA_REMOTE"]["bootstrap_servers"],
    'security_protocol': config["KAFKA_REMOTE"]["security_protocol"],
    'sasl_mechanism': config["KAFKA_REMOTE"]["sasl_mechanism"],
    'sasl_plain_username': config["KAFKA_REMOTE"]["sasl_username"],
    'sasl_plain_password': config["KAFKA_REMOTE"]["sasl_password"],
    'group_id': 'remote_consumer_group',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False
}

def json_deserializer(m):
    return json.loads(m.decode('utf-8')) if m else None

consumer = KafkaConsumer(
    config["KAFKA_REMOTE"]["topic"],
    **remote_conf,
    value_deserializer=json_deserializer
)

logger.info(f"[{consumer}] Subscribed to topic: {config['KAFKA_REMOTE']['topic']}")

def consume_remote():
    try:
        for msg in consumer:
            print(f"[{consumer}] {msg.topic}:{msg.partition}:{msg.offset} value={msg.value}")
            yield msg.value
    except KeyboardInterrupt:
        logger.info(f"Stopping {consumer}...")
    finally:
        consumer.close()

if __name__ == "__main__":
    for _ in consume_remote():
        pass
