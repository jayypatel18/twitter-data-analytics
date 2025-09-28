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

# -----------------------------------------------------------------------------
# FIREWALL SETUP
# -----------------------------------------------------------------------------

# macOS (Master) - Allow required ports
# sudo ufw allow 7077  # Master port
# sudo ufw allow 8080  # Master UI port
# sudo ufw allow 4040  # Application UI port

# Windows (Workers) - Open Windows Firewall
# - Allow Java applications through firewall
# - Ensure network profile is set to Private

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
