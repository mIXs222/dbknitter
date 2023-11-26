#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and PIP if not present
sudo apt-get install -y python3 python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install direct_redis
pip3 install git+https://github.com/RedisLabs/direct_redis.git
