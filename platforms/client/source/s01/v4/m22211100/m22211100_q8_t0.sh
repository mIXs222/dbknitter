#!/bin/bash

# Update package list
apt-get update

# Install Python
apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pymongo pandas

# Clone the repository with the modified Redis-py library
git clone https://github.com/datasets-io/direct_redis.git
cd direct_redis

# Install the modified Redis-py library
python3 setup.py install

# Go back to the original directory
cd ..
