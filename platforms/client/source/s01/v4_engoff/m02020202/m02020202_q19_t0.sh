#!/bin/bash

# Update package list
sudo apt-get update

# Install Python
sudo apt-get install -y python3 python3-pip

# Install pymysql, pandas, and direct_redis
pip3 install pymysql pandas git+https://github.com/RedisLabsModules/direct_redis.git

# For redis-py, which direct_redis depends on
pip3 install redis

# Additional dependency that may be required for pandas read_msgpack
pip3 install msgpack

# Run the discounted_revenue python script
python3 discounted_revenue.py
