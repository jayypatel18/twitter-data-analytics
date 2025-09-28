# Real-Time Tweet Sentiment Analysis with Docker, Kafka and Spark Streaming

A real-time Twitter sentiment analysis pipeline using Apache Spark, Kafka, and machine learning.

## Architecture

- **Producer**: Fetches Twitter data and sends to Kafka
- **Consumer**: Processes data from Kafka using Spark Streaming and ML pipeline
- **Storage Options**: Local files, MongoDB, or Delta Lake

![image](https://github.com/Wazzabeee/pyspark-etl-twitter/blob/main/images/flow.png?raw=true)

## Prerequisites

### All Machines
- Java 8 or 11
- Python 3.8+
- Apache Spark 4.0.1
- Apache Kafka

### Master Node (macOS)
- MongoDB (optional, for consumer_mongo.py)
- Git

### Worker Nodes (Windows)
- Same Java and Spark versions as master

## Installation

### Master Node (macOS)

1. **Install dependencies:**
```bash
brew install apache-spark kafka
pip install -r requirements.txt
```

2. **Start Kafka and Zookeeper:**
```bash
# Start Zookeeper
brew services start zookeeper

# Start Kafka
brew services start kafka

# Or use Docker Compose
docker-compose -f zk-single-kafka-single.yml up -d
```

3. **Get your IP address:**
```bash
ifconfig | grep "inet " | grep -v 127.0.0.1
# Note your IP (e.g., 192.168.1.100)
```

4. **Start Spark Master:**
```bash
$SPARK_HOME/sbin/start-master.sh --host YOUR_IP_ADDRESS
```

5. **Access Spark Master UI:**
- Open http://YOUR_IP_ADDRESS:8080
- Note the Master URL (e.g., spark://192.168.1.100:7077)

### Worker Nodes (Windows)

1. **Install Java and Spark:**
- Download and install Java 11
- Download Apache Spark 4.0.1 and extract
- Set JAVA_HOME and SPARK_HOME environment variables

2. **Start Spark Worker:**
```cmd
%SPARK_HOME%\bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://MASTER_IP:7077
```

Replace `MASTER_IP` with your master node's IP address.

## Configuration

### Environment Variables (.env)

Create a `.env` file in the project root:

```env
# Twitter API Credentials
CONSUMERKEY=your_consumer_key
CONSUMERSECRET=your_consumer_secret
ACCESSTOKEN=your_access_token
ACCESSTOKENSECRET=your_access_token_secret
BEARERTOKEN=your_bearer_token

# MongoDB (optional)
MONGOACCESS=mongodb://localhost:27017

# Delta Lake path (optional)
DELTAPATH=/path/to/delta/storage
```

### Update Consumer Configuration

Modify the consumer files to use cluster mode:

```python
spark = SparkSession 
    .builder 
    .master("spark://YOUR_MASTER_IP:7077") 
    .appName("TwitterSentimentAnalysis") 
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") 
    .getOrCreate()
```

## Running the Pipeline

### 1. Start Data Producer

Choose one of the producers:

**Option A: Twitter API Producer**
```bash
python producer.py
```

**Option B: Search API Producer (Rate-limited)**
```bash
python producer_search.py
```

**Option C: Sample Data Producer (No API required)**
```bash
python producer_sample.py
```

### 2. Start Consumer

Choose your preferred storage backend:

**Console Output:**
```bash
python consumer.py
```

**Local File Storage:**
```bash
python consumer_local.py
```

**MongoDB Storage:**
```bash
python consumer_mongo.py
```

**Delta Lake Storage:**
```bash
python consumer_delta.py
```

## Monitoring

### Spark Cluster Monitoring

1. **Master UI**: http://YOUR_MASTER_IP:8080
   - View cluster resources
   - Monitor worker nodes
   - See submitted applications

2. **Application UI**: http://YOUR_MASTER_IP:4040
   - Monitor job execution
   - View streaming statistics
   - Debug performance issues

### Kafka Monitoring

**List topics:**
```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

**Monitor messages:**
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic twitter --from-beginning
```

## Troubleshooting

### Common Issues

1. **Port Conflicts**
   - Spark Master: 7077, 8080
   - Spark UI: 4040
   - Kafka: 9092
   - Zookeeper: 2181

2. **Network Connectivity**
   - Ensure all machines are on the same network
   - Check firewall settings
   - Verify IP addresses are accessible

3. **Version Compatibility**
   - Use same Spark version across all nodes
   - Ensure Java versions are compatible
   - Check Scala version (2.13 for Spark 4.0.1)

### Firewall Configuration

**macOS (Master):**
```bash
# Allow Spark ports
sudo ufw allow 7077
sudo ufw allow 8080
sudo ufw allow 4040
```

**Windows (Workers):**
- Open Windows Firewall
- Allow Spark worker ports (usually dynamic)
- Ensure network profile is set to Private

## Performance Tuning

### Spark Configuration

Add these configs for better performance:

```python
.config("spark.sql.adaptive.enabled", "true") 
.config("spark.sql.adaptive.coalescePartitions.enabled", "true") 
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") 
.config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
```

### Kafka Configuration

For high throughput:
```bash
# In server.properties
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
```

## Project Structure

```
├── consumer.py              # Console output consumer
├── consumer_local.py        # Local file storage consumer
├── consumer_mongo.py        # MongoDB storage consumer
├── consumer_delta.py        # Delta Lake storage consumer
├── producer.py              # Twitter streaming producer
├── producer_search.py       # Twitter search API producer
├── producer_sample.py       # Sample data producer
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables
├── zk-single-kafka-single.yml  # Docker Compose for Kafka
├── pre_trained_model/       # ML model files
├── locally_saved_results/   # Sample data and results
└── README.md               # This file
```

## Technologies

- **Python**: Core programming language
- **Apache Spark**: Distributed data processing
- **Apache Kafka**: Real-time data streaming
- **Docker**: Containerization for Kafka/Zookeeper
- **MongoDB**: Document storage (optional)
- **Delta Lake**: Data lake storage (optional)
- **Machine Learning**: Pre-trained sentiment analysis model

## Contact

For issues and questions:
1. Check the troubleshooting section
2. Review Spark and Kafka documentation
3. Open an issue on GitHub
