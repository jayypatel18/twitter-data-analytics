import os
import findspark
from pyspark.sql import SparkSession

if __name__ == "__main__":
    findspark.init()

    # Config
    spark = SparkSession \
        .builder \
        .appName("DebugDistributedFiles") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6") \
        .getOrCreate()

    # Spark Context
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    def debug_files():
        import os
        import socket
        hostname = socket.gethostname()
        cwd = os.getcwd()
        files = os.listdir('.')
        
        result = f"Host: {hostname}, CWD: {cwd}, Files: {files}"
        
        # Check if pre_trained_model directory exists
        if os.path.exists('./pre_trained_model'):
            model_files = os.listdir('./pre_trained_model')
            result += f", Model dir exists with files: {model_files}"
        else:
            result += ", Model dir does not exist"
            
        return result

    # Create RDD and run the debug function on each worker
    rdd = sc.parallelize([1], 1)  # Single partition to test one worker
    results = rdd.map(lambda x: debug_files()).collect()
    
    print("=== WORKER DEBUG RESULTS ===")
    for result in results:
        print(result)
    print("=== END DEBUG RESULTS ===")
    
    spark.stop()
