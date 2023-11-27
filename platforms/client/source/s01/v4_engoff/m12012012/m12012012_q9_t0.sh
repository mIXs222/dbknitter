#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3, pip and Python development files (if not already installed)
sudo apt-get install -y python3 python3-pip python3-dev

# Install MySQL client
sudo apt-get install -y default-libmysqlclient-dev

# Upgrade pip to the latest version
python3 -m pip install --upgrade pip

# Install Pandas, PyMySQL, Pymongo, and direct_redis
pip3 install pandas pymysql pymongo direct_redis
