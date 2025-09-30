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
        
        # MongoDB connection
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['twitter_emotions']
        self.collection = self.db['emotion_analysis']
        
        # Spark Session
        self.spark = SparkSession \
            .builder \
            .appName("TwitterEmotionAnalysis") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
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
        Write DataFrame to MongoDB
        """
        try:
            # Convert to Pandas DataFrame for easier MongoDB insertion
            pandas_df = df.toPandas()
            
            # Convert to records and insert
            records = pandas_df.to_dict('records')
            
            if records:
                self.collection.insert_many(records)
                logger.info(f"Inserted {len(records)} records to MongoDB")
            
        except Exception as e:
            logger.error(f"Error writing to MongoDB: {e}")
    
    def analyze_emotion_trends(self, df, epoch_id):
        """
        Perform real-time emotion trend analysis
        """
        try:
            # Convert to Pandas for analysis
            pandas_df = df.toPandas()
            
            if not pandas_df.empty:
                # Emotion distribution
                emotion_counts = pandas_df['dominant_emotion'].value_counts()
                
                # High confidence emotions
                high_conf_emotions = pandas_df[pandas_df['emotion_confidence'] > 0.7]
                
                # Sentiment distribution
                sentiment_dist = pandas_df['sentiment_label'].value_counts()
                
                print("\n" + "="*50)
                print("REAL-TIME EMOTION ANALYSIS")
                print("="*50)
                print(f"Total Tweets Processed: {len(pandas_df)}")
                print(f"High Confidence Emotions: {len(high_conf_emotions)}")
                
                print("\nDominant Emotion Distribution:")
                for emotion, count in emotion_counts.head().items():
                    print(f"  {emotion.upper()}: {count}")
                
                print(f"\nSentiment Distribution:")
                for sentiment, count in sentiment_dist.items():
                    print(f"  {sentiment.upper()}: {count}")
                
                # Find most emotional tweet
                if not pandas_df.empty:
                    most_emotional = pandas_df.loc[pandas_df['emotion_confidence'].idxmax()]
                    print(f"\nMost Emotional Tweet:")
                    print(f"  Emotion: {most_emotional['dominant_emotion'].upper()} ({most_emotional['emotion_confidence']:.2f})")
                    print(f"  Text: {most_emotional['original_text'][:100]}...")
                
                print("="*50 + "\n")
                
        except Exception as e:
            logger.error(f"Error in emotion trend analysis: {e}")
    
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
            
            # Wait for termination
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
