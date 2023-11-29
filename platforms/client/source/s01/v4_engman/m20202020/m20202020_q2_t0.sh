#!/bin/bash

# Update package lists
apt-get update

# Ensure pip is installed
apt-get install -y python3-pip

# Install the pymysql library
pip3 install pymysql

# Install pandas
pip3 install pandas

# Install direct_redis library
pip3 install git+https://github.com/nirmata/direct_redis.git

# Run the python script
python3 execute_query.py
