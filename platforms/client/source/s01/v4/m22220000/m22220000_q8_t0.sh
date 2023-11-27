#!/bin/bash
# File: setup.sh

# Update package list
apt-get update

# Install python3 and python3-pip if they are not available
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas redis

# Install direct_redis separately (assuming this is the correct command as 'direct_redis' is not a standard package)
pip3 install git+https://github.com/username/direct_redis.git # Replace with the real URL for direct_redis

# Run the python script to execute the query
python3 query_execution.py
