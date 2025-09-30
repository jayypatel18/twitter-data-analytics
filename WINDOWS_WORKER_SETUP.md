# Windows Worker Node Setup Guide

## For Your Friends' Windows Laptops

### 1. Install Python (if not installed)
```bash
# Download Python from https://www.python.org/downloads/
# Make sure to check "Add Python to PATH" during installation
```

### 2. Verify Python Installation
```cmd
# Open Command Prompt and test:
python --version
# Should show: Python 3.x.x

# Also test:
python -c "print('Python is working')"
```

### 3. Install Required Python Packages
```cmd
# Install PySpark and other dependencies
pip install pyspark==3.5.6
pip install findspark
pip install kafka-python
pip install pymongo
```

### 4. Set Environment Variables (Important!)
```cmd
# Method 1: Set via Command Prompt (temporary)
set PYSPARK_PYTHON=python
set PYSPARK_DRIVER_PYTHON=python

# Method 2: Set via System Properties (permanent)
# 1. Right-click "This PC" → Properties
# 2. Advanced System Settings → Environment Variables
# 3. Add these system variables:
#    - PYSPARK_PYTHON = python
#    - PYSPARK_DRIVER_PYTHON = python
```

### 5. Download and Install Spark
```cmd
# Download Spark 3.5.6 from:
# https://spark.apache.org/downloads.html
# Choose: Spark 3.5.6, Pre-built for Apache Hadoop 3.3

# Extract to: C:\spark-3.5.6-bin-hadoop3
# Set environment variable:
# SPARK_HOME = C:\spark-3.5.6-bin-hadoop3
```

### 6. Start Worker Node
```cmd
# Open Command Prompt as Administrator
# Navigate to Spark directory
cd C:\spark-3.5.6-bin-hadoop3

# Start worker (replace MASTER_IP with actual master IP)
bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://MASTER_IP:7077

# Example:
bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://192.168.1.100:7077
```

### 7. Verify Worker Connection
- Open browser and go to: `http://MASTER_IP:8080`
- You should see your Windows machine listed as a worker
- Worker UI available at: `http://YOUR_WINDOWS_IP:8081`

### 8. Troubleshooting Common Issues

#### Problem: "python3 not found"
```cmd
# Solution: Use 'python' instead of 'python3' on Windows
# This is handled by our Spark configuration
```

#### Problem: "JAVA_HOME not set"
```cmd
# Install Java 8 or 11
# Set JAVA_HOME environment variable
# Example: JAVA_HOME = C:\Program Files\Java\jdk-11.0.x
```

#### Problem: Worker cannot connect to master
```cmd
# Check Windows Firewall
# Allow Java applications through firewall
# Test connection: telnet MASTER_IP 7077
```

#### Problem: Permission denied
```cmd
# Run Command Prompt as Administrator
# Make sure Spark directory has proper permissions
```

### 9. Optional: Create Batch File for Easy Startup

Create `start_worker.bat`:
```batch
@echo off
set PYSPARK_PYTHON=python
set PYSPARK_DRIVER_PYTHON=python
set SPARK_HOME=C:\spark-3.5.6-bin-hadoop3
cd %SPARK_HOME%
echo Starting Spark Worker...
echo Master IP: %1
bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://%1:7077
pause
```

Usage:
```cmd
start_worker.bat 192.168.1.100
```

### 10. Network Configuration
- Ensure Windows machines are on the same network as master
- Use Private network profile (not Public)
- Disable Windows Defender Firewall for Private networks (temporarily for testing)

---

## Commands Summary for Windows Workers:

```cmd
# 1. Set environment variables
set PYSPARK_PYTHON=python
set PYSPARK_DRIVER_PYTHON=python

# 2. Start worker
C:\spark-3.5.6-bin-hadoop3\bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://MASTER_IP:7077

# 3. Monitor
# Worker UI: http://YOUR_IP:8081
# Master UI: http://MASTER_IP:8080
```