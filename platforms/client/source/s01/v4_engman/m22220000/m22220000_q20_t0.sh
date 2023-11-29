#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip
sudo apt-get install -y python3 python3-pip

# Install pymysql, pandas, and redis
pip3 install pymysql pandas

# Clone and install direct_redis (you may need Git installed for this)
git clone https://github.com/priestc/direct_redis.git
cd direct_redis
python3 setup.py install
cd .. && rm -rf direct_redis

# Running the python script
python3 query.py
