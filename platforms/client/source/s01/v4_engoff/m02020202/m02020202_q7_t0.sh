#!/bin/bash

# Update package list and upgrade existing packages
sudo apt-get update
sudo apt-get -y upgrade

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pymysql and pandas through pip
pip3 install pymysql pandas

# Assuming 'direct_redis' is available on PyPi. If it's not, this command needs to be adjusted.
pip3 install direct_redis

# You are now ready to run the Python script
# python3 execute_query.py
