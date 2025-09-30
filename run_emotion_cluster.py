#!/usr/bin/env python3
"""
Cluster deployment script for Twitter Emotion Analysis Consumer
Run this script to deploy the emotion analysis consumer on a Spark cluster
"""

import os
import sys
import subprocess

def get_master_ip():
    """Get the master node IP address"""
    try:
        # Try to get IP from ifconfig
        result = subprocess.run(['ifconfig'], capture_output=True, text=True)
        lines = result.stdout.split('\n')
        
        for line in lines:
            if 'inet ' in line and '127.0.0.1' not in line and 'inet 192.168.' in line:
                ip = line.split()[1]
                return ip
        
        # Fallback - ask user
        ip = input("Enter your master node IP address: ")
        return ip
        
    except Exception as e:
        print(f"Could not auto-detect IP: {e}")
        ip = input("Enter your master node IP address: ")
        return ip

def deploy_to_cluster():
    """Deploy the emotion analysis consumer to Spark cluster"""
    
    # Get configuration
    master_ip = get_master_ip()
    master_url = f"spark://{master_ip}:7077"
    
    print(f"Deploying to Spark cluster at: {master_url}")
    print("Starting Twitter Emotion Analysis Consumer on cluster...")
    print("="*70)
    
    # Spark submit command for cluster deployment with Windows worker support
    spark_submit_cmd = [
        "/opt/homebrew/Cellar/apache-spark1/3.5.6/bin/spark-submit",
        "--master", master_url,
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
        "--conf", "spark.default.parallelism=8",
        "--conf", "spark.sql.shuffle.partitions=8",
        "--conf", "spark.network.timeout=300s",
        "--conf", "spark.executor.heartbeatInterval=30s",
        "--conf", "spark.executor.memory=2g",
        "--conf", "spark.executor.cores=2",
        "--conf", "spark.driver.memory=1g",
        "--conf", "spark.driver.maxResultSize=1g",
        "--conf", "spark.pyspark.python=python",
        "--conf", "spark.pyspark.driver.python=python",
        "--total-executor-cores", "4",
        "/Users/jaypatel/pyspark-etl-twitter/consumer_emotion_mongo.py"
    ]
    


    
    print("Command being executed:")
    print(" ".join(spark_submit_cmd))
    print("\n" + "="*70)
    
    try:
        # Execute the command
        subprocess.run(spark_submit_cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running spark-submit: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nStopping cluster deployment...")
        sys.exit(0)

if __name__ == "__main__":
    print("Twitter Emotion Analysis - Cluster Deployment")
    print("Multi-node deployment with Windows workers support")
    print("="*70)
    
    # Check if MongoDB is running
    print("Prerequisites:")
    print("1. MongoDB should be running on master node (port 27017)")
    print("   → brew services start mongodb-community")
    print("2. Kafka should be running with 'twitter' topic (NOT Docker)")
    print("   → brew services start zookeeper")
    print("   → brew services start kafka")
    print("3. Spark cluster should be active with workers connected")
    print("   → Check: http://YOUR_IP:8080")
    print("4. Producer should be sending data to Kafka")
    print("   → python producer_sample.py")
    print("\n💡 Run 'python test_kafka_connection.py' to verify Kafka setup")
    
    proceed = input("\nAll prerequisites met? (y/n): ")
    if proceed.lower() != 'y':
        print("Please ensure all prerequisites are met before deployment.")
        sys.exit(1)
    
    deploy_to_cluster()
