bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

# Set Python environment for Spark workers
export PYSPARK_PYTHON=/Users/jaypatel/pyspark-etl-twitter/venv/bin/python

# Local mode (single machine)
/opt/homebrew/Cellar/apache-spark1/3.5.6/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 /Users/jaypatel/pyspark-etl-twitter/consumer.py

# Cluster mode (distributed across master + workers)
# Option 1: Distribute the model files to all nodes
tar -czf pre_trained_model.tar.gz pre_trained_model/
/opt/homebrew/Cellar/apache-spark1/3.5.6/bin/spark-submit \
  --master spark://192.168.0.118:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --conf "spark.pyspark.python=/Users/jaypatel/pyspark-etl-twitter/venv/bin/python" \
  --archives pre_trained_model.tar.gz \
  /Users/jaypatel/pyspark-etl-twitter/consumer.py

# Option 2: Simple cluster mode (requires model copied to all worker nodes manually)
/opt/homebrew/Cellar/apache-spark1/3.5.6/bin/spark-submit \
  --master spark://192.168.0.118:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --conf "spark.pyspark.python=/Users/jaypatel/pyspark-etl-twitter/venv/bin/python" \
  /Users/jaypatel/pyspark-etl-twitter/consumer.py



/opt/homebrew/Cellar/apache-spark1/3.5.6/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  /Users/jaypatel/pyspark-etl-twitter/consumer.py