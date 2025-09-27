import os
import json
import time
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'twitter'

def send_sample_data():
    """
    Send sample data from locally_saved_results to Kafka
    """
    data_dir = 'locally_saved_results'
    
    # Get all JSON files in the directory
    json_files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    
    for json_file in json_files:
        file_path = os.path.join(data_dir, json_file)
        print(f"Reading data from: {json_file}")
        
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    data = json.loads(line.strip())
                    print(f"Sending data: {str(data)[:100]}...")
                    producer.send(topic_name, value=json.dumps(data).encode('utf-8'))
                    time.sleep(0.005)  # Small delay between messages
                    
        except Exception as e:
            print(f"Error reading {json_file}: {e}")
    
    print("Finished sending all sample data to Kafka")

if __name__ == '__main__':
    send_sample_data()
