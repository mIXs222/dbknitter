#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip3
sudo apt-get install -y python3 python3-pip

# Install the required libraries
pip3 install pymysql pymongo pandas direct-redis

# Create a directory for the CSV output if it doesn't already exist
mkdir -p /path/to/csv_output_directory

# Run the Python script
python3 /path/to/query_code.py
