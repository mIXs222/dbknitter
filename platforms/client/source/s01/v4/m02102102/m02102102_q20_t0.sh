#!/bin/bash

# Update package lists
sudo apt-get update

# Install MySQL client
sudo apt-get install -y default-mysql-client

# Install MongoDB client
sudo apt-get install -y mongodb-clients

# Install Redis client
sudo apt-get install -y redis-tools

# Set up Python environment
sudo apt-get install -y python3 python3-pip

# Install Python packages
pip3 install pymysql pymongo pandas direct_redis
