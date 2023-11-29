#!/bin/bash

# Update package lists
sudo apt-get update

# Install MongoDB
sudo apt-get install -y mongodb

# Install Redis
sudo apt-get install -y redis-server

# Install Python pip
sudo apt-get install -y python3-pip

# Install Python libraries
pip3 install pymongo pandas redis direct_redis
