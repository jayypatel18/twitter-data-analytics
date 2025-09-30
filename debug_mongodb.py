#!/usr/bin/env python3
"""
MongoDB Debug Script - Check what data is stored for the dashboard
"""

from pymongo import MongoClient
import json
from datetime import datetime

def check_mongodb_data():
    """Check what data is in MongoDB collections"""
    
    try:
        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client['twitter_emotions']
        
        print("🔍 MONGODB DEBUG REPORT")
        print("=" * 50)
        
        # Check emotion_analysis collection
        emotion_collection = db['emotion_analysis']
        emotion_count = emotion_collection.count_documents({})
        print(f"\n📄 emotion_analysis collection:")
        print(f"   Total documents: {emotion_count}")
        
        if emotion_count > 0:
            # Get a sample document
            sample_doc = emotion_collection.find_one()
            print(f"   Sample document keys: {list(sample_doc.keys())}")
            
            # Get latest 5 documents
            latest_docs = list(emotion_collection.find().sort("_id", -1).limit(5))
            print(f"   Latest 5 document IDs: {[str(doc['_id']) for doc in latest_docs]}")
        
        # Check real_time_stats collection
        stats_collection = db['real_time_stats']
        stats_count = stats_collection.count_documents({})
        print(f"\n📊 real_time_stats collection:")
        print(f"   Total documents: {stats_count}")
        
        if stats_count > 0:
            # Get the current stats
            current_stats = stats_collection.find_one({"stats_type": "current"})
            if current_stats:
                print(f"   Current stats found!")
                print(f"   Total tweets: {current_stats.get('total_tweets_processed', 'N/A')}")
                print(f"   Batch ID: {current_stats.get('batch_id', 'N/A')}")
                print(f"   Timestamp: {current_stats.get('timestamp', 'N/A')}")
                print(f"   Emotion distribution: {current_stats.get('emotion_distribution', {})}")
            else:
                print("   No current stats found!")
                # Show all stats documents
                all_stats = list(stats_collection.find())
                for stat in all_stats:
                    print(f"   Stats doc: {stat.get('stats_type', 'unknown')} - {stat.get('batch_id', 'N/A')}")
        
        # Check database collections
        collections = db.list_collection_names()
        print(f"\n📚 All collections in database:")
        for collection_name in collections:
            count = db[collection_name].count_documents({})
            print(f"   {collection_name}: {count} documents")
        
        print("\n" + "=" * 50)
        print("✅ MongoDB debug complete!")
        
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
    
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    check_mongodb_data()
