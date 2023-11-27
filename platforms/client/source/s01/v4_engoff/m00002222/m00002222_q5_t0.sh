#!/bin/bash

# Update the package lists
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install Pandas library
pip3 install pandas

# Install direct_redis for connecting to Redis
pip3 install git+https://github.com/RedisGears/direct_redis_py.git
