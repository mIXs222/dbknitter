#!/bin/bash

# Update package list
apt-get update

# Install Python and PIP
apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install direct_redis
pip3 install git+https://github.com/redis/direct_redis.git

# Install additional dependencies for msgpack (used by direct_redis)
pip3 install msgpack-python
