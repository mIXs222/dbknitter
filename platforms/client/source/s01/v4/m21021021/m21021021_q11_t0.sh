#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python pip (Ubuntu/Debian system)
sudo apt-get install -y python3-pip

# Install MongoDB client
sudo apt-get install -y mongodb-clients

# Install Redis client
sudo apt-get install -y redis-tools

# Python dependencies
pip3 install pymongo pandas direct_redis pandarallel
