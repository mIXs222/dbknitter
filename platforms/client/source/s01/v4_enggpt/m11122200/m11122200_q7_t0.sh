#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 and pip, if not already installed
sudo apt-get install -y python3 python3-pip

# Install MongoDB and its dependencies
sudo apt-get install -y mongodb

# Install Redis and its dependencies
sudo apt-get install -y redis-server

# Install the Python libraries required
pip3 install pymysql pymongo pandas direct_redis
