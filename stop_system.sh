#!/bin/bash

echo "🛑 Stopping Twitter Emotion Analysis System"
echo "==========================================="

# Function to stop a component
stop_component() {
    local component=$1
    local pid_file="${component}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat $pid_file)
        if ps -p $pid > /dev/null 2>&1; then
            echo "🛑 Stopping $component (PID: $pid)..."
            kill $pid
            rm $pid_file
            echo "✓ $component stopped"
        else
            echo "⚠️  $component was not running"
            rm $pid_file
        fi
    else
        echo "⚠️  No PID file found for $component"
    fi
}

# Stop all components
stop_component "consumer"
stop_component "producer"
stop_component "dashboard"

# Kill any remaining Python processes related to the project
echo "🧹 Cleaning up any remaining processes..."
pkill -f "consumer_emotion_mongo.py"
pkill -f "producer_sample.py"
pkill -f "emotion_dashboard.py"

echo ""
echo "✅ All components stopped!"
echo "💾 Data remains in MongoDB for future analysis"
