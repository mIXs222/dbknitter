#!/bin/bash

# Update package list and install Python if necessary
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Pandas and direct_redis Python packages
pip3 install pandas direct-redis

# Run the Python script
python3 query_analysis.py
