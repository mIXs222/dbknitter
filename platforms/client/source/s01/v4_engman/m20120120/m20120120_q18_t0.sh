#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and Pip, if not already installed
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas numpy

# Cloning and installing direct_redis as it might not be available on PyPI
git clone https://github.com/instaclustr/direct_redis.git
cd direct_redis
python3 setup.py install
cd ..
rm -rf direct_redis
