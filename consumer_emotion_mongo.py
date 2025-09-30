import os
import re
import json
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf, col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, MapType, IntegerType
from pymongo import MongoClient
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmotionAnalysisConsumer:
    def __init__(self):
        findspark.init()
        
        # MongoDB connection - Use environment variable or default to localhost
        mongo_host = os.getenv('MONGO_HOST', 'localhost')
        mongo_port = int(os.getenv('MONGO_PORT', '27017'))
        
        print(f"Connecting to MongoDB at {mongo_host}:{mongo_port}")
        self.mongo_client = MongoClient(f'mongodb://{mongo_host}:{mongo_port}/')
        self.db = self.mongo_client['twitter_emotions']
        self.collection = self.db['emotion_analysis']
        
        # Spark Session - Configured for multi-node cluster with Windows workers
        self.spark = SparkSession \
            .builder \
            .appName("TwitterEmotionAnalysis") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.network.timeout", "300s") \
            .config("spark.executor.heartbeatInterval", "30s") \
            .config("spark.pyspark.python", "python") \
            .config("spark.pyspark.driver.python", "python") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel('ERROR')
        
        # Enhanced schema for emotion data
        self.schema = StructType([
            StructField("tweet_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("original_text", StringType(), True),
            StructField("cleaned_data", StringType(), True),
            StructField("sentiment_prediction", FloatType(), True),
            StructField("sentiment_label", StringType(), True),
            StructField("emotions", MapType(StringType(), FloatType()), True),
            StructField("dominant_emotion", StringType(), True),
            StructField("emotion_confidence", FloatType(), True),
            StructField("topic", StringType(), True),
            StructField("location", StringType(), True),
            StructField("engagement_score", FloatType(), True),
            StructField("virality_potential", FloatType(), True)
        ])
    
    def write_to_mongo(self, df, epoch_id):
        """
        Write DataFrame to MongoDB - Optimized for distributed processing
        """
        try:
            # Check if DataFrame is empty before processing
            if df.rdd.isEmpty():
                logger.info("No data to process in this batch")
                return
            
            # Collect data on driver node (master) to avoid serialization issues
            # This ensures MongoDB operations happen only on the master node
            collected_data = df.collect()
            
            # Convert to records and insert
            records = [row.asDict() for row in collected_data]
            
            if records:
                # Batch insert to MongoDB
                self.collection.insert_many(records)
                logger.info(f"Batch {epoch_id}: Inserted {len(records)} records to MongoDB")
            
        except Exception as e:
            logger.error(f"Error writing to MongoDB in batch {epoch_id}: {e}")
    
    def analyze_emotion_trends(self, df, epoch_id):
        """
        Perform real-time emotion trend analysis - Distributed processing optimized
        """
        try:            
            print(f"\n🔍 Processing batch {epoch_id}...")
            
            # Check if DataFrame is empty before processing
            if df.rdd.isEmpty():
                print(f"⚠️  No data received in batch {epoch_id} - waiting for producer data...")
                return
            
            # Use Spark operations for distributed processing, then collect results
            emotion_counts = df.groupBy("dominant_emotion").count().collect()
            sentiment_counts = df.groupBy("sentiment_label").count().collect()
            total_count = df.count()
            
            # Get high confidence emotions count
            high_conf_count = df.filter(col("emotion_confidence") > 0.7).count()
            
            # Find most emotional tweet (collect only one row)
            most_emotional_row = df.orderBy(col("emotion_confidence").desc()).first()
            
            if total_count > 0:
                print("\n" + "="*50)
                print(f"REAL-TIME EMOTION ANALYSIS (Batch {epoch_id})")
                print("="*50)
                print(f"Total Tweets Processed: {total_count}")
                print(f"High Confidence Emotions: {high_conf_count}")
                
                print("\nDominant Emotion Distribution:")
                for row in sorted(emotion_counts, key=lambda x: x['count'], reverse=True):
                    emotion = row['dominant_emotion'] or 'UNKNOWN'
                    print(f"  {emotion.upper()}: {row['count']}")
                
                print(f"\nSentiment Distribution:")
                for row in sorted(sentiment_counts, key=lambda x: x['count'], reverse=True):
                    sentiment = row['sentiment_label'] or 'UNKNOWN'
                    print(f"  {sentiment.upper()}: {row['count']}")
                
                # Display most emotional tweet
                if most_emotional_row and most_emotional_row['dominant_emotion']:
                    print(f"\nMost Emotional Tweet:")
                    emotion = most_emotional_row['dominant_emotion'] or 'UNKNOWN'
                    confidence = most_emotional_row['emotion_confidence'] or 0.0
                    text = most_emotional_row['original_text'] or 'No text available'
                    print(f"  Emotion: {emotion.upper()} ({confidence:.2f})")
                    print(f"  Text: {text[:100]}...")
                
                print("="*50 + "\n")
                
        except Exception as e:
            logger.error(f"Error in emotion trend analysis: {e}")
    
    def start_processing(self):
        """
        Start the emotion analysis streaming process
        """
        try:
            # Read from Kafka - Optimized for cluster processing
            kafka_servers = os.getenv('KAFKA_SERVERS', 'localhost:9092')
            print(f"Connecting to Kafka at: {kafka_servers}")
            print("Subscribing to topic: twitter")
            
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_servers) \
                .option("subscribe", "twitter") \
                .option("startingOffsets", "earliest") \
                .option("kafka.consumer.commit.groupid", "emotion-analysis-group") \
                .option("maxOffsetsPerTrigger", "100") \
                .option("failOnDataLoss", "false") \
                .option("kafka.session.timeout.ms", "30000") \
                .option("kafka.request.timeout.ms", "40000") \
                .load() \
                .selectExpr("CAST(value AS STRING) as json_data")
            
            # Parse JSON data
            parsed_df = df.withColumn("data", from_json(col("json_data"), self.schema)) \
                         .select("data.*") \
                         .withColumn("processed_at", current_timestamp())
            
            print("Starting streaming queries...")
            print("Waiting for data from Kafka topic 'twitter'...")
            
            # Console output with emotion analysis (start this first for debugging)
            console_query = parsed_df.writeStream \
                .foreachBatch(self.analyze_emotion_trends) \
                .outputMode("append") \
                .trigger(processingTime='5 seconds') \
                .option("checkpointLocation", "/tmp/emotion_console_checkpoint") \
                .start()
            
            # MongoDB sink
            mongo_query = parsed_df.writeStream \
                .foreachBatch(self.write_to_mongo) \
                .outputMode("append") \
                .trigger(processingTime='10 seconds') \
                .option("checkpointLocation", "/tmp/emotion_mongo_checkpoint") \
                .start()
            
            print("Streaming queries started successfully!")
            print("Monitor at: http://localhost:4040 (if running locally)")
            print("Press Ctrl+C to stop...")
            
            # Wait for termination
            try:
                console_query.awaitTermination()
            except KeyboardInterrupt:
                print("\nStopping streaming...")
                console_query.stop()
                mongo_query.stop()
            
        except Exception as e:
            logger.error(f"Error in streaming process: {e}")
        finally:
            self.spark.stop()
            self.mongo_client.close()

if __name__ == "__main__":
    print("Starting Twitter Emotion Analysis Consumer...")
    print("Features: Real-time emotion detection, MongoDB storage, trend analysis")
    print("Emotions: Joy, Anger, Fear, Sadness, Disgust, Surprise, Trust, Anticipation")
    print("="*70)
    
    consumer = EmotionAnalysisConsumer()
    consumer.start_processing()
