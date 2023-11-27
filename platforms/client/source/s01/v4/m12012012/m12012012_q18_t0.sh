#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and pip (if not already installed)
sudo apt-get install -y python3 python3-pip

# Install pymysql to connect to MySQL
pip3 install pymysql

# Install pymongo to connect to MongoDB
pip3 install pymongo

# Install pandas for data manipulation
pip3 install pandas

# Install direct_redis for interacting with Redis
pip3 install git+https://github.com/RedisBloom/redis-py.git#egg=direct_redis

# Reminder to run the Python script
echo "Dependencies installed. Run the Python script with 'python3 query_exec.py'."
