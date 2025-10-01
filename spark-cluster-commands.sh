#!/bin/bash

# =============================================================================
# SPARK CLUSTER SETUP COMMANDS
# =============================================================================

# -----------------------------------------------------------------------------
# MASTER NODE (macOS - Your Laptop)
# -----------------------------------------------------------------------------

# 1. Get your IP address (note this for worker nodes)
ifconfig | grep "inet " | grep -v 127.0.0.1

# 2. Start Spark Master (this opens UI on port 8080)
$SPARK_HOME/sbin/start-master.sh --host YOUR_IP_ADDRESS

# Alternative: Start master with specific IP
# /opt/homebrew/Cellar/apache-spark1/3.5.6/sbin/start-master.sh --host 192.168.1.100

# 3. Access Master UI in browser
# http://YOUR_IP_ADDRESS:8080
# Example: http://192.168.1.100:8080

# 4. Note the Master URL from the UI (usually spark://YOUR_IP:7077)

# 5. Stop master (when needed)
# $SPARK_HOME/sbin/stop-master.sh

# -----------------------------------------------------------------------------
# WORKER NODES (Windows - Friends' Laptops)
# -----------------------------------------------------------------------------

# Commands for Windows (run in Command Prompt):

# 1. Start Worker Node 1
# %SPARK_HOME%\bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://MASTER_IP:7077

# 2. Start Worker Node 2 (on second laptop)
# %SPARK_HOME%\bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://MASTER_IP:7077

# Example:
# %SPARK_HOME%\bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://192.168.1.100:7077

# -----------------------------------------------------------------------------
# ALTERNATIVE: PowerShell Commands for Windows
# -----------------------------------------------------------------------------

# In PowerShell:
# & "$env:SPARK_HOME\bin\spark-class.cmd" org.apache.spark.deploy.worker.Worker spark://MASTER_IP:7077

# -----------------------------------------------------------------------------
# WORKER NODES (macOS - If Friends Have Mac Laptops)
# -----------------------------------------------------------------------------

# Commands for macOS (run in Terminal):

# 1. Start Worker Node 1
# $SPARK_HOME/sbin/start-worker.sh spark://MASTER_IP:7077

# 2. Alternative: Start worker with specific options
# $SPARK_HOME/sbin/start-worker.sh --cores 2 --memory 2g spark://MASTER_IP:7077

# Example with full path:
# /opt/homebrew/Cellar/apache-spark1/3.5.6/sbin/start-worker.sh spark://192.168.1.100:7077

# Or if using Homebrew Spark:
# /usr/local/bin/spark-class org.apache.spark.deploy.worker.Worker spark://192.168.1.100:7077

# Stop worker (when needed):
# $SPARK_HOME/sbin/stop-worker.sh

# -----------------------------------------------------------------------------
# WORKER NODES (Linux - If Using Linux Systems)
# -----------------------------------------------------------------------------

# Commands for Linux (run in Terminal):

# 1. Start Worker Node
# $SPARK_HOME/sbin/start-worker.sh spark://MASTER_IP:7077

# 2. With specific configuration
# $SPARK_HOME/sbin/start-worker.sh --cores 4 --memory 4g spark://MASTER_IP:7077

# Example:
# /opt/spark/sbin/start-worker.sh spark://192.168.1.100:7077

# Stop worker:
# $SPARK_HOME/sbin/stop-worker.sh

# -----------------------------------------------------------------------------
# CONSUMER APPLICATION
# -----------------------------------------------------------------------------

# Run consumer pointing to cluster master
/opt/homebrew/Cellar/apache-spark1/3.5.6/bin/spark-submit \
  --master spark://YOUR_IP:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  /Users/jaypatel/pyspark-etl-twitter/consumer.py

# Alternative: Run with Python directly (if packages are configured in code)
# /Users/jaypatel/pyspark-etl-twitter/venv/bin/python /Users/jaypatel/pyspark-etl-twitter/consumer.py

# -----------------------------------------------------------------------------
# PRODUCER APPLICATION
# -----------------------------------------------------------------------------

# Start sample data producer
/Users/jaypatel/pyspark-etl-twitter/venv/bin/python /Users/jaypatel/pyspark-etl-twitter/producer_sample.py

# -----------------------------------------------------------------------------
# MONITORING URLs
# -----------------------------------------------------------------------------

# Spark Master UI: http://YOUR_IP:8080
# - Shows cluster resources
# - Worker node status  
# - Running applications

# Spark Application UI: http://YOUR_IP:4040
# - Job execution details
# - Streaming statistics
# - Performance metrics

# Worker UI (for each worker node):
# http://WORKER_IP:8081
# - Individual worker status
# - Resource usage
# - Running executors

# -----------------------------------------------------------------------------
# FIREWALL SETUP
# -----------------------------------------------------------------------------

# macOS (Master) - Allow required ports
# sudo ufw allow 7077  # Master port
# sudo ufw allow 8080  # Master UI port
# sudo ufw allow 4040  # Application UI port

# macOS (Workers) - If using Mac as worker nodes
# sudo ufw allow 7077  # Worker communication port
# sudo ufw allow 8081  # Worker UI port (default)

# Windows (Workers) - Open Windows Firewall
# - Allow Java applications through firewall
# - Ensure network profile is set to Private
# - Open ports: 7077, 8081

# Linux (Workers) - Firewall setup
# sudo ufw allow 7077  # Worker communication port
# sudo ufw allow 8081  # Worker UI port

# -----------------------------------------------------------------------------
# TROUBLESHOOTING
# -----------------------------------------------------------------------------

# Check if master is running
# ps aux | grep spark

# Check network connectivity from worker to master
# telnet MASTER_IP 7077

# View master logs
# tail -f $SPARK_HOME/logs/spark-*-org.apache.spark.deploy.master.Master-*.out

# View worker logs (on Windows)
# type %SPARK_HOME%\logs\spark-*-org.apache.spark.deploy.worker.Worker-*.out
