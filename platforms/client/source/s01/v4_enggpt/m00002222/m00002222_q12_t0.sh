#!/bin/bash
set -e

# Ensure Python3 and pip are installed; if not, install them
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pandas library for data manipulation
pip3 install pandas

# Install Redis and direct_redis library for the our custom connection to Redis
sudo apt-get install -y redis-server
pip3 install direct_redis

# Make sure Redis server is started
sudo systemctl start redis-server

# Run the Python script
python3 query_analysis.py
