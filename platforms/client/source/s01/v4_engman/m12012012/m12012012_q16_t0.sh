#!/bin/bash

# Update the package list
apt-get update

# Install pip for Python package management
apt-get install -y python3-pip

# Install Python MySQL client (pymysql)
pip3 install pymysql

# Install Python MongoDB client (pymongo)
pip3 install pymongo

# Install pandas for data manipulation
pip3 install pandas

# Install direct_redis to work with Redis
pip3 install git+https://github.com/redis/direct_redis.git
