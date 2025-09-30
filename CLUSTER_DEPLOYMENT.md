# Multi-Node Cluster Deployment Guide for Twitter Emotion Analysis

## Overview
This guide helps you deploy the Twitter Emotion Analysis consumer on a multi-node Spark cluster with Windows worker nodes.

## Key Changes Made for Cluster Deployment

### 1. Enhanced Spark Configuration
```python
# Optimized for distributed processing
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
.config("spark.sql.execution.arrow.pyspark.enabled", "true")
.config("spark.default.parallelism", "8")
.config("spark.sql.shuffle.partitions", "8")
.config("spark.network.timeout", "300s")
.config("spark.executor.heartbeatInterval", "30s")
```

### 2. MongoDB Connection (Master Node Only)
- MongoDB operations now happen only on the master node
- Uses `df.collect()` instead of `toPandas()` to avoid serialization issues
- Environment variable support: `MONGO_HOST`, `MONGO_PORT`

### 3. Kafka Configuration
- Added cluster-optimized Kafka settings
- Consumer group ID for better partition management
- Environment variable support: `KAFKA_SERVERS`

### 4. Distributed Processing Optimizations
- Uses Spark DataFrame operations instead of Pandas
- Data collection happens on driver node
- Improved error handling for empty DataFrames

## Deployment Steps

### Step 1: Start Spark Cluster
```bash
# On macOS Master:
./spark-cluster-commands.sh

# Get your IP
ifconfig | grep "inet " | grep -v 127.0.0.1

# Start master
$SPARK_HOME/sbin/start-master.sh --host YOUR_IP
```

### Step 2: Connect Windows Workers
```cmd
# On each Windows laptop:
%SPARK_HOME%\bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://MASTER_IP:7077
```

### Step 3: Start Prerequisites
```bash
# Start MongoDB (on master node)
brew services start mongodb-community

# Start Kafka & Zookeeper
docker-compose -f zk-single-kafka-single.yml up -d

# Start producer
python producer_sample.py
```

### Step 4: Deploy Emotion Analysis Consumer

#### Option A: Using the deployment script
```bash
python run_emotion_cluster.py
```

#### Option B: Manual spark-submit
```bash
spark-submit \
  --master spark://YOUR_IP:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=2 \
  --total-executor-cores=4 \
  consumer_emotion_mongo.py
```

## Environment Variables (Optional)

Create a `.env` file or set environment variables:
```bash
# MongoDB Configuration (if not on localhost)
export MONGO_HOST=192.168.1.100
export MONGO_PORT=27017

# Kafka Configuration (if not on localhost)
export KAFKA_SERVERS=192.168.1.100:9092
```

## Monitoring URLs

- **Spark Master UI**: http://YOUR_IP:8080
- **Application UI**: http://YOUR_IP:4040
- **Worker UIs**: http://WORKER_IP:8081

## Key Benefits of Multi-Node Setup

1. **Distributed Processing**: Emotion analysis runs across multiple cores
2. **Better Throughput**: Can handle more tweets per second
3. **Resource Utilization**: Uses CPU/memory from worker nodes
4. **Fault Tolerance**: Spark handles worker node failures
5. **Scalability**: Easy to add more worker nodes

## Troubleshooting

### Common Issues:

1. **Worker Connection Failed**
   ```bash
   # Check network connectivity
   telnet MASTER_IP 7077
   ```

2. **MongoDB Connection Error**
   ```bash
   # Ensure MongoDB is running on master
   brew services list | grep mongodb
   ```

3. **Kafka Connection Error**
   ```bash
   # Check Kafka status
   docker-compose ps
   ```

4. **Out of Memory Errors**
   ```bash
   # Increase executor memory
   --conf spark.executor.memory=4g
   ```

### Performance Tuning:

```bash
# For high-throughput scenarios
spark-submit \
  --master spark://YOUR_IP:7077 \
  --executor-memory 4g \
  --executor-cores 4 \
  --total-executor-cores 8 \
  --conf spark.sql.shuffle.partitions=16 \
  --conf spark.default.parallelism=16 \
  consumer_emotion_mongo.py
```

## File Structure After Changes

```
/Users/jaypatel/pyspark-etl-twitter/
├── consumer_emotion_mongo.py      # Modified for cluster deployment
├── run_emotion_cluster.py         # New deployment script
├── spark-cluster-commands.sh      # Cluster setup commands
└── CLUSTER_DEPLOYMENT.md          # This guide
```

## Next Steps

1. Test with sample data using `producer_sample.py`
2. Monitor performance via Spark UI
3. Scale up by adding more worker nodes
4. Optimize based on your data volume and processing requirements

---

**Note**: The emotion analysis consumer will now distribute processing across your cluster while keeping MongoDB operations centralized on the master node for data consistency.
