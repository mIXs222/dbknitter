#!/bin/bash
# Bash script to install dependencies for query.py

# Update package lists
apt-get update

# Install Python (just assuming Python is not installed on the system)
apt-get install -y python3

# Install pip (assuming pip is not present)
apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pandas direct-redis
