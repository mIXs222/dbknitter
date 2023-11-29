#!/bin/bash

# Update apt-get just in case
sudo apt-get update

# Install MySQL client
sudo apt-get install -y default-libmysqlclient-dev

# Install Redis tools
sudo apt-get install -y redis-tools

# Upgrade pip
sudo python3 -m pip install --upgrade pip

# Install Python library dependencies
sudo pip3 install pymysql pymongo pandas

# Install direct_redis separately since it might not be available in pip
git clone https://github.com/RedisDirect/direct_redis.git
cd direct_redis
sudo pip3 install .
cd ..
rm -rf direct_redis
