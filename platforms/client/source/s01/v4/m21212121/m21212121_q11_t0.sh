#!/bin/bash

# Update package lists
apt-get update

# Install Python and PIP
apt-get install -y python3 python3-pip

# Install Pymongo
pip3 install pymongo

# Install Redis client with the JSON feature
pip3 install git+https://github.com/RedisJSON/redis-py.git

# Install pandas
pip3 install pandas
