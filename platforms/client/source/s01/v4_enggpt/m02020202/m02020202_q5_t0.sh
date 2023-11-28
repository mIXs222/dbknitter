#!/bin/bash

# Update system package index
sudo apt-get update

# Install pip if not already installed
sudo apt-get install -y python3-pip

# Ensure pymysql is installed
pip3 install pymysql

# Install direct_redis which includes pandas
pip3 install git+https://github.com/filmaj/direct_redis.git

# Install Redis if needed for other operations (Python connectivity doesn't need it)
# sudo apt-get install -y redis-server
