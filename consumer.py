import re
import findspark
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


if __name__ == "__main__":
    findspark.init()
    
    import os
    from pyspark import SparkFiles
    
    # Path to the pre-trained model
    # In cluster mode with --archives, the model is extracted to the working directory
    # Try distributed path first, fallback to local path
    if os.path.exists('./pre_trained_model'):
        path_to_model = './pre_trained_model'  # Distributed mode
    else:
        path_to_model = './pre_trained_model'  # Local mode (same path)

    # Config
    spark = SparkSession \
        .builder \
        .appName("TwitterSentimentAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6") \
        .getOrCreate()

    # Spark Context
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # Schema for the incoming data
    schema = StructType([StructField("message", StringType())])

    # Read the data from kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .option("startingOffsets", "latest") \
        .option("header", "true") \
        .load() \
        .selectExpr("CAST(value AS STRING) as message")

    df = df \
        .withColumn("value", from_json("message", schema))

    # Pre-processing the data
    pre_process = udf(
        lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', x.lower().strip()).split(), ArrayType(StringType())
    )
    df = df.withColumn("cleaned_data", pre_process(df.message)).dropna()

    # Load the pre-trained model
    pipeline_model = PipelineModel.load(path_to_model)
    # Make predictions
    prediction = pipeline_model.transform(df)
    # Select the columns of interest
    prediction = prediction.select(prediction.message, prediction.prediction)

    # Print prediction in console
    prediction \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .start() \
        .awaitTermination()
