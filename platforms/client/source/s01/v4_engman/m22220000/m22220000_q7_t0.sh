#!/bin/bash

# Updating package list
sudo apt-get update

# Installing Python3 and Pip if not installed
sudo apt-get install -y python3 python3-pip

# Installing Python packages
pip3 install pandas pymysql redis

# Clone the 'direct_redis' repository (as it's not available in PyPI)
git clone https://github.com/RedisDirect/direct_redis.git
cd direct_redis
python3 setup.py install
cd ..
rm -rf direct_redis

# Running Python script (assuming the script above is saved as run_query.py)
python3 run_query.py
