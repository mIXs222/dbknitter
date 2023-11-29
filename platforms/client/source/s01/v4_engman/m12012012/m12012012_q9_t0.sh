#!/bin/bash
# Install Python dependencies for the query_code.py script

# Update package list and install Python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct-redis

# Install the additional dependency for Pandas (to handle msgpack for Redis)
pip3 install msgpack-python
