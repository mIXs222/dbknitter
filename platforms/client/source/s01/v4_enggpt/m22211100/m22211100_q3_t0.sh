#!/bin/bash

# Update package list and install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pymongo

# Set permissions and execute the Python script
chmod +x query_analysis.py
python3 query_analysis.py
