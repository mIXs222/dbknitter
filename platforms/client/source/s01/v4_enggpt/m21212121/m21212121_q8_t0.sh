#!/bin/bash

# Update package list
apt-get update

# Install pip and Python dev tools
apt-get install -y python3-pip python3-dev

# Install MongoDB driver `pymongo`
pip3 install pymongo

# Install `direct_redis` for Redis connection
pip3 install git+https://github.com/RedisLabs/direct_redis.git

# Install pandas
pip3 install pandas
