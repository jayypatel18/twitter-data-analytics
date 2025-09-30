#!/usr/bin/env python3
"""
Kafka Connection Test Script
Tests Kafka connectivity before running the emotion analysis consumer
"""

import subprocess
import time
import sys
import os

def check_kafka_running():
    """Check if Kafka is running"""
    try:
        # Check if Kafka process is running
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        if 'kafka' in result.stdout.lower():
            print("✅ Kafka process found running")
            return True
        else:
            print("❌ Kafka process not found")
            return False
    except Exception as e:
        print(f"Error checking Kafka: {e}")
        return False

def check_kafka_topics():
    """List Kafka topics"""
    try:
        print("\n📋 Listing Kafka topics...")
        # Find Kafka installation path
        kafka_home = "/Users/jaypatel/Downloads/kafka_2.13-3.9.1/bin"
        if not os.path.exists(f"{kafka_home}/kafka-topics.sh"):
            kafka_home = "/opt/homebrew/bin"
            if not os.path.exists(f"{kafka_home}/kafka-topics"):
                kafka_home = "/usr/local/bin"
        
        cmd = [f"{kafka_home}/kafka-topics.sh", "--list", "--bootstrap-server", "localhost:9092"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            topics = result.stdout.strip().split('\n')
            print(f"Available topics: {topics}")
            
            if 'twitter' in topics:
                print("✅ 'twitter' topic found")
                return True
            else:
                print("❌ 'twitter' topic not found")
                print("Creating 'twitter' topic...")
                create_cmd = [
                    f"{kafka_home}/kafka-topics.sh",
                    "--create",
                    "--topic", "twitter",
                    "--bootstrap-server", "localhost:9092",
                    "--partitions", "3",
                    "--replication-factor", "1"
                ]
                create_result = subprocess.run(create_cmd, capture_output=True, text=True)
                if create_result.returncode == 0:
                    print("✅ 'twitter' topic created successfully")
                    return True
                else:
                    print(f"❌ Failed to create topic: {create_result.stderr}")
                    return False
        else:
            print(f"❌ Failed to list topics: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Timeout connecting to Kafka")
        return False
    except Exception as e:
        print(f"❌ Error checking Kafka topics: {e}")
        return False

def test_producer():
    """Test if producer is sending data"""
    try:
        print("\n📡 Testing if producer is sending data...")
        print("This will consume a few messages from the 'twitter' topic...")
        
        kafka_home = "/Users/jaypatel/Downloads/kafka_2.13-3.9.1/bin"
        if not os.path.exists(f"{kafka_home}/kafka-console-consumer.sh"):
            kafka_home = "/opt/homebrew/bin"
            if not os.path.exists(f"{kafka_home}/kafka-console-consumer"):
                kafka_home = "/usr/local/bin"
        
        cmd = [
            f"{kafka_home}/kafka-console-consumer.sh",
            "--bootstrap-server", "localhost:9092",
            "--topic", "twitter",
            "--from-beginning",
            "--max-messages", "3"
        ]
        
        print("Running: " + " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        
        if result.stdout.strip():
            print("✅ Data found in 'twitter' topic:")
            lines = result.stdout.strip().split('\n')
            for i, line in enumerate(lines[:3], 1):
                print(f"  Message {i}: {line[:100]}...")
            return True
        else:
            print("❌ No data found in 'twitter' topic")
            print("Make sure your producer is running!")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Timeout reading from Kafka topic")
        return False
    except Exception as e:
        print(f"❌ Error testing producer: {e}")
        return False

def main():
    print("🔍 KAFKA CONNECTIVITY TEST")
    print("=" * 50)
    
    # Check if Kafka is running
    if not check_kafka_running():
        print("\n💡 To start Kafka on Mac:")
        print("brew services start kafka")
        print("brew services start zookeeper")
        return False
    
    # Check topics
    if not check_kafka_topics():
        return False
    
    # Test producer data
    if not test_producer():
        print("\n💡 To start a producer:")
        print("python producer.py")
        print("# or")
        print("python producer_sample.py")
        return False
    
    print("\n🎉 All Kafka tests passed!")
    print("You can now run the emotion analysis consumer:")
    print("python consumer_emotion_mongo.py")
    return True

if __name__ == "__main__":
    main()
