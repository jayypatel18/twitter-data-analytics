import os
import json
import time
from kafka import KafkaProducer
import logging
import random
import uuid
from datetime import datetime

logging.basicConfig(level=logging.INFO)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'twitter'

EMOTIONS = ['joy', 'anger', 'fear', 'sadness', 'disgust', 'surprise', 'trust', 'anticipation']
TOPICS = ['technology', 'sports', 'politics', 'entertainment', 'business', 'health', 'education', 'travel']
LOCATIONS = ['New York', 'California', 'Texas', 'London', 'Tokyo', 'Delhi', 'Sydney', 'Toronto']

def reconstruct_original_text(cleaned_data):
    """
    Reconstruct approximate original text from cleaned data
    """
    return ' '.join(cleaned_data)

def simulate_emotion_metrics(prediction):
    """
    Simulate realistic emotion detection based on sentiment prediction
    """
    emotions = {}
    
    if prediction == 1.0:
        emotions['anger'] = random.uniform(0.3, 0.8)
        emotions['sadness'] = random.uniform(0.2, 0.6)
        emotions['fear'] = random.uniform(0.1, 0.4)
        emotions['disgust'] = random.uniform(0.1, 0.3)
        emotions['joy'] = random.uniform(0.0, 0.2)
        emotions['trust'] = random.uniform(0.0, 0.3)
        emotions['surprise'] = random.uniform(0.1, 0.4)
        emotions['anticipation'] = random.uniform(0.1, 0.3)
    else:
        emotions['joy'] = random.uniform(0.4, 0.9)
        emotions['trust'] = random.uniform(0.3, 0.7)
        emotions['anticipation'] = random.uniform(0.2, 0.6)
        emotions['surprise'] = random.uniform(0.1, 0.5)
        emotions['anger'] = random.uniform(0.0, 0.2)
        emotions['sadness'] = random.uniform(0.0, 0.2)
        emotions['fear'] = random.uniform(0.0, 0.1)
        emotions['disgust'] = random.uniform(0.0, 0.1)
    
    dominant_emotion = max(emotions, key=emotions.get)
    
    return emotions, dominant_emotion

def send_sample_data():
    """
    Send enhanced sample data with emotion detection to Kafka
    """
    data_dir = 'locally_saved_results'
    
    json_files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    
    for json_file in json_files:
        file_path = os.path.join(data_dir, json_file)
        print(f"Reading data from: {json_file}")
        
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    original_data = json.loads(line.strip())
                    
                    original_text = reconstruct_original_text(original_data['cleaned_data'])
                    emotions, dominant_emotion = simulate_emotion_metrics(original_data['prediction'])
                    enhanced_data = {
                        'tweet_id': str(uuid.uuid4()),
                        'timestamp': datetime.now().isoformat(),
                        'original_text': original_text,
                        'cleaned_data': original_data['cleaned_data'],
                        'sentiment_prediction': original_data['prediction'],
                        'sentiment_label': 'negative' if original_data['prediction'] == 1.0 else 'positive',
                        'emotions': emotions,
                        'dominant_emotion': dominant_emotion,
                        'emotion_confidence': emotions[dominant_emotion],
                        'topic': random.choice(TOPICS),
                        'location': random.choice(LOCATIONS),
                        'engagement_score': random.uniform(0.1, 1.0),
                        'virality_potential': random.uniform(0.0, 1.0)
                    }
                    
                    print(f"Sending tweet: {original_text[:50]}... | Emotion: {dominant_emotion}")
                    producer.send(topic_name, value=json.dumps(enhanced_data).encode('utf-8'))
                    time.sleep(0.9)
                    
        except Exception as e:
            print(f"Error reading {json_file}: {e}")
    
    print("Finished sending all enhanced sample data to Kafka")

if __name__ == '__main__':
    send_sample_data()
