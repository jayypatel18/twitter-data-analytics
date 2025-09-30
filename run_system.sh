#!/bin/bash

echo " Starting Twitter Emotion Analysis System"
echo "==========================================="

# Function to run processes in the background
run_component() {
    local component=$1
    local script=$2
    local log_file=$3
    
    echo " Starting $component..."
    python $script > $log_file 2>&1 &
    local pid=$!
    echo "✓ $component started (PID: $pid)"
    echo $pid > "${component}.pid"
}

# Create logs directory
mkdir -p logs

echo " Step 1: Starting Emotion Consumer..."
run_component "consumer" "consumer_emotion_mongo.py" "logs/consumer.log"

sleep 5

echo " Step 2: Starting Sample Data Producer..."
run_component "producer" "producer_sample.py" "logs/producer.log"

sleep 3

echo " Step 3: Starting Dashboard..."
run_component "dashboard" "emotion_dashboard.py" "logs/dashboard.log"

echo ""
echo " All components started!"
echo "================================"
echo " Consumer: Processing tweets with emotion analysis"
echo " Producer: Sending sample tweets to Kafka"
echo " Dashboard: http://localhost:8050"
echo ""
echo " To monitor logs:"
echo "   Consumer: tail -f logs/consumer.log"
echo "   Producer: tail -f logs/producer.log"
echo "   Dashboard: tail -f logs/dashboard.log"
echo ""
echo " To stop all components:"
echo "   ./stop_system.sh"
