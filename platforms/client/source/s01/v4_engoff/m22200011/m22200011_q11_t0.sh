#!/bin/bash

# Update package list
apt-get update

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install Pandas
pip3 install pandas

# Install PyMySQL
pip3 install pymysql

# Install direct_redis, may require installing git if not available
apt-get install -y git
pip3 install git+https://github.com/RedisDirect/direct_redis.git
