#!/bin/bash

echo " Setting up Twitter Emotion Analysis Project"
echo "================================================"

# Install Python packages
echo " Installing Python packages..."
pip install -r requirements.txt

# Start MongoDB (if not running)
echo " Starting MongoDB..."
if ! pgrep -x "mongod" > /dev/null; then
    echo "Starting MongoDB..."
    brew services start mongodb-community
else
    echo "MongoDB is already running"
fi

# Start Kafka and Zookeeper
echo " Starting Kafka and Zookeeper..."
if ! pgrep -f "kafka" > /dev/null; then
    echo "Starting Kafka services..."
    docker-compose -f zk-single-kafka-single.yml up -d
    sleep 10
else
    echo "Kafka is already running"
fi

# Create Kafka topic
echo " Creating Kafka topic..."
docker exec -it kafka kafka-topics.sh --create --topic twitter --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists

echo " Setup complete!"
echo ""
echo " Next steps:"
echo "1. Run the producer: python producer_sample.py"
echo "2. Run the consumer: python consumer_emotion_mongo.py"
echo "3. Start the dashboard: python emotion_dashboard.py"
echo ""
echo " Dashboard will be available at: http://localhost:8050"
