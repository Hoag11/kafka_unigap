from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from config_loader import config
from logger import logger
from remote_consumer import consume_remote

producer = KafkaProducer(
    bootstrap_servers=config["KAFKA_LOCAL"]["bootstrap_servers"],
    security_protocol=config["KAFKA_LOCAL"]["security_protocol"],
    sasl_mechanism=config["KAFKA_LOCAL"]["sasl_mechanism"],
    sasl_plain_username=config["KAFKA_LOCAL"]["sasl_username"],
    sasl_plain_password=config["KAFKA_LOCAL"]["sasl_password"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)

topic = config["KAFKA_LOCAL"]["topic"]

def send_to_local_kafka():
    for message in consume_remote():
        future = producer.send(topic, value=message)
        try:
            record_metadata = future.get(timeout=10)
            logger.info(f"Sent to Kafka Local: {record_metadata.topic} [{record_metadata.partition}] Offset {record_metadata.offset}")
            print(f'Sent to Kafka Local: {record_metadata.topic} [{record_metadata.partition}] Offset {record_metadata.offset}')
        except KafkaError as e:
            logger.error("Error:", exc_info=e)

if __name__ == "__main__":
    send_to_local_kafka()
    producer.flush()
