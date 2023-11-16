#!/bin/bash

# Update the package lists
sudo apt-get update -y

# Install Python3 and pip if not already installed
sudo apt-get install python3 python3-pip -y

# Install pandas and direct_redis within Python3
pip3 install pandas
pip3 install git+https://github.com/hmartinez/direct_redis

# Make sure that the Python script has execution permissions
chmod +x query_redis.py

# Run the Python script
python3 query_redis.py
