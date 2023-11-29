#!/bin/bash

# Update package lists
apt-get update

# Install Python3 and pip if not already installed
apt-get install -y python3 python3-pip

# Install Python packages
pip3 install pymysql pymongo pandas direct_redis

# Give execution rights to the Python script
chmod +x query.py

# Running the Python script should now execute your query
# You can run it using: ./query.py
