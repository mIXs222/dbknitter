#!/bin/bash

# Ensure pip, Python package manager, is installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymongo to connect to MongoDB
pip3 install pymongo

# Install pandas for data manipulation
pip3 install pandas

# Install redis-py and direct_redis for connection to Redis
pip3 install redis
pip3 install direct-redis

echo "All dependencies installed successfully"
