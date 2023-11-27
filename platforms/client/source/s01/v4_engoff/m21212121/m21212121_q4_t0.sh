#!/bin/bash

# Update package list and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python MongoDB driver "pymongo"
pip3 install pymongo

# Install Pandas for data manipulation
pip3 install pandas

# Install direct_redis for Redis database access
pip3 install direct_redis
