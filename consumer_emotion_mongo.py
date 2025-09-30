import os
import re
import json
import findspark
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf, col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, MapType, IntegerType
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmotionAnalysisConsumer:
    def __init__(self):
        findspark.init()
        
        self.total_tweets_processed = 0
        self.total_high_confidence = 0
        
        mongo_host = os.getenv('MONGO_HOST', 'localhost')
        mongo_port = int(os.getenv('MONGO_PORT', '27017'))
        
        print(f"Connecting to MongoDB at {mongo_host}:{mongo_port}")
        self.mongo_client = MongoClient(f'mongodb://{mongo_host}:{mongo_port}/')
        self.db = self.mongo_client['twitter_emotions']
        self.collection = self.db['emotion_analysis']
        self.stats_collection = self.db['real_time_stats']
        
        self.spark = SparkSession \
            .builder \
            .appName("TwitterEmotionAnalysis") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel('ERROR')
        
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
        Write DataFrame to MongoDB
        """
        try:
            pandas_df = df.toPandas()
            records = pandas_df.to_dict('records')
            
            if records:
                self.collection.insert_many(records)
                logger.info(f"Inserted {len(records)} records to MongoDB")
            
        except Exception as e:
            logger.error(f"Error writing to MongoDB: {e}")
    
    def analyze_emotion_trends(self, df, epoch_id):
        """
        Perform real-time emotion trend analysis - Old pandas approach with cumulative counter
        """
        try:
            pandas_df = df.toPandas()
            
            if not pandas_df.empty:
                batch_size = len(pandas_df)
                self.total_tweets_processed += batch_size
                emotion_counts = pandas_df['dominant_emotion'].value_counts()
                high_conf_emotions = pandas_df[pandas_df['emotion_confidence'] > 0.7]
                high_conf_count = len(high_conf_emotions)
                self.total_high_confidence += high_conf_count
                sentiment_dist = pandas_df['sentiment_label'].value_counts()
                
                print("\n" + "="*50)
                print(f"REAL-TIME EMOTION ANALYSIS (Batch {epoch_id})")
                print("="*50)
                print(f"Batch Size: {batch_size}")
                print(f"TOTAL TWEETS PROCESSED: {self.total_tweets_processed}")
                print(f"Total High Confidence Emotions: {self.total_high_confidence}")
                
                print("\nDominant Emotion Distribution (Current Batch):")
                for emotion, count in emotion_counts.head().items():
                    print(f"  {emotion.upper()}: {count}")
                
                print(f"\nSentiment Distribution (Current Batch):")
                for sentiment, count in sentiment_dist.items():
                    print(f"  {sentiment.upper()}: {count}")
                
                # Find most emotional tweet
                if not pandas_df.empty:
                    most_emotional = pandas_df.loc[pandas_df['emotion_confidence'].idxmax()]
                    print(f"\nMost Emotional Tweet in Batch:")
                    print(f"  Emotion: {most_emotional['dominant_emotion'].upper()} ({most_emotional['emotion_confidence']:.2f})")
                    print(f"  Text: {most_emotional['original_text'][:100]}...")
                
                print("="*50 + "\n")

                self.update_dashboard_stats(pandas_df, epoch_id)
            else:
                print(f"\n⚠️  No data received in batch {epoch_id} - waiting for producer data...")
                
        except Exception as e:
            logger.error(f"Error in emotion trend analysis: {e}")
    
    def update_dashboard_stats(self, pandas_df, epoch_id):
        """
        Update real-time statistics in MongoDB for dashboard consumption
        """
        try:
            cumulative_emotion_dist = {}
            cumulative_sentiment_dist = {}
            
            try:
                emotion_pipeline = [
                    {"$group": {"_id": "$dominant_emotion", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ]
                emotion_aggregation = list(self.collection.aggregate(emotion_pipeline))
                cumulative_emotion_dist = {item['_id']: item['count'] for item in emotion_aggregation if item['_id']}
                
                sentiment_pipeline = [
                    {"$group": {"_id": "$sentiment_label", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ]
                sentiment_aggregation = list(self.collection.aggregate(sentiment_pipeline))
                cumulative_sentiment_dist = {item['_id']: item['count'] for item in sentiment_aggregation if item['_id']}
                
            except Exception as agg_error:
                logger.warning(f"Error getting cumulative distributions: {agg_error}")
                cumulative_emotion_dist = {k: int(v) for k, v in pandas_df['dominant_emotion'].value_counts().to_dict().items()}
                cumulative_sentiment_dist = {k: int(v) for k, v in pandas_df['sentiment_label'].value_counts().to_dict().items()}
            
            stats_doc = {
                "timestamp": datetime.now(),
                "batch_id": int(epoch_id),
                "batch_size": int(len(pandas_df)),
                "total_tweets_processed": int(self.total_tweets_processed),
                "total_high_confidence": int(self.total_high_confidence),
                "current_batch_emotion_distribution": {k: int(v) for k, v in pandas_df['dominant_emotion'].value_counts().to_dict().items()},
                "current_batch_sentiment_distribution": {k: int(v) for k, v in pandas_df['sentiment_label'].value_counts().to_dict().items()},
                "cumulative_emotion_distribution": cumulative_emotion_dist,
                "cumulative_sentiment_distribution": cumulative_sentiment_dist,
                "high_confidence_emotions": int(len(pandas_df[pandas_df['emotion_confidence'] > 0.7])),
                "avg_emotion_confidence": float(pandas_df['emotion_confidence'].mean()) if not pandas_df.empty else 0.0,
                "most_emotional_tweet": {
                    "emotion": str(pandas_df.loc[pandas_df['emotion_confidence'].idxmax(), 'dominant_emotion']) if not pandas_df.empty else 'unknown',
                    "confidence": float(pandas_df['emotion_confidence'].max()) if not pandas_df.empty else 0.0,
                    "text": str(pandas_df.loc[pandas_df['emotion_confidence'].idxmax(), 'original_text'][:100]) if not pandas_df.empty else ''
                }
            }
            
            self.stats_collection.replace_one(
                {"stats_type": "current"}, 
                {**stats_doc, "stats_type": "current"}, 
                upsert=True
            )
            
            logger.info(f"Updated dashboard stats for batch {epoch_id} - Total tweets: {self.total_tweets_processed}")
            
        except Exception as e:
            logger.error(f"Error updating dashboard stats: {e}")
    
    def start_processing(self):
        """
        Start the emotion analysis streaming process
        """
        try:
            # Read from Kafka
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "twitter") \
                .option("startingOffsets", "latest") \
                .load() \
                .selectExpr("CAST(value AS STRING) as json_data")
            
            # Parse JSON data
            parsed_df = df.withColumn("data", from_json(col("json_data"), self.schema)) \
                         .select("data.*") \
                         .withColumn("processed_at", current_timestamp())
            
            # MongoDB sink
            mongo_query = parsed_df.writeStream \
                .foreachBatch(self.write_to_mongo) \
                .outputMode("append") \
                .trigger(processingTime='10 seconds') \
                .start()
            
            # Console output with emotion analysis
            console_query = parsed_df.writeStream \
                .foreachBatch(self.analyze_emotion_trends) \
                .outputMode("append") \
                .trigger(processingTime='10 seconds') \
                .start()
            
            mongo_query.awaitTermination()
            console_query.awaitTermination()
            
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