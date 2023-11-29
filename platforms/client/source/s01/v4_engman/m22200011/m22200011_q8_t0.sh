#!/bin/bash

# Update and install system-wide packages
sudo apt update
sudo apt install -y python3-pip

# Install Python package dependencies
pip3 install pymysql pymongo pandas 'redis==4.3.4'

# download direct_redis from GitHub and install
git clone https://github.com/dongjinleekr/direct-redis.git 
cd direct-redis 
python3 setup.py install

# Note: redis version fixed at 4.3.4 due to compatibility with direct_redis at the time of writing.
# You should check for compatibility issues before running this script.
