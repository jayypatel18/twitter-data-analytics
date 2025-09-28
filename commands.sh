bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

/opt/homebrew/Cellar/apache-spark1/3.5.6/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 /Users/jaypatel/pyspark-etl-twitter/consumer.py

