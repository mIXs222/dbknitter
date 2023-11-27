#!/bin/bash

# Update package lists
apt-get update

# Install Python3, pip and other system dependencies
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql
pip3 install pymongo
pip3 install pandas

# Additional steps to install direct_redis
git clone https://github.com/yoyonel/direct_redis.git
cd direct_redis
python3 setup.py install
cd ..
rm -rf direct_redis
