#!/bin/bash

# setup.sh

# Update package index
apt-get update

# Install pip if it's not available
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pymongo pandas direct-redis

# Run the Python script
python3 query.py
