#!/bin/bash

KAFKA_CONTAINER="kafka-0"

# Kafka Remote & Local Brokers
KAFKA_REMOTE_BROKER="113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294"
KAFKA_LOCAL_BROKER="kafka-0:29092,kafka-1:29092,kafka-2:29092"

# Tên topic
REMOTE_TOPIC="product_view"
LOCAL_TOPIC="product_view_local"


# Tạo topic trên Kafka Local nếu chưa tồn tại
echo "Check exist and create topic cho Kafka Local..."
docker exec -it kafka-0 kafka-topics --bootstrap-server $KAFKA_LOCAL_BROKER --list | grep -q $LOCAL_TOPIC || \
docker exec -it kafka-0 kafka-topics --create --bootstrap-server $KAFKA_LOCAL_BROKER --topic $LOCAL_TOPIC --partitions 3 --replication-factor 3

echo "Running Remote Consumer..."
gnome-terminal -- bash -c "python3 src/remote_consumer.py --consumer-id=1; exec bash"

# Chạy Local Producer
echo "Running Local Producer..."
gnome-terminal -- bash -c "python3 src/local_producer.py --consumer-id=0; exec bash"

# Chạy Local Consumer
echo "Running Local Consumer để lưu vào MongoDB..."
gnome-terminal -- bash -c "python3 src/local_consumer.py --consumer-id=0; exec bash"

echo "Success!"