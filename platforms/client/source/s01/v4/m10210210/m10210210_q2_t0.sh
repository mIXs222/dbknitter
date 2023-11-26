#!/bin/bash

# Install Python and PIP if not installed
# Assuming a Debian-based system
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pandas pymysql pymongo redis msgpack

# Remind user to run the Python script
echo "Please run the Python script with 'python3 query_code.py'"
