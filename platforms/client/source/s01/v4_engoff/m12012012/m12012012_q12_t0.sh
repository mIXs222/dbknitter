#!/bin/bash

# Update repository
sudo apt-get update

# Install Python 3 and pip
sudo apt-get install -y python3 python3-pip

# Install MongoDB driver pymongo
pip3 install pymongo

# Install direct_redis package for Redis connection
pip3 install git+https://github.com/marcingl/direct_redis.git

# Install pandas
pip3 install pandas
