#!/bin/bash

# Create a new file to store all dependencies
echo "pymysql" > requirements.txt
echo "sqlalchemy" >> requirements.txt
echo "pandas" >> requirements.txt
echo "direct_redis" >> requirements.txt

# Update system package index
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Redis server
apt-get install -y redis-server

# Install Python dependencies
pip3 install -r requirements.txt
