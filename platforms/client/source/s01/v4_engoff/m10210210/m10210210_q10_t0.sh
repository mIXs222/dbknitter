#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas redis

# Assuming 'direct_redis' is a custom-made Python module or is hosted somewhere accessible via pip.
# If 'direct_redis' is not available via pip, you would need to obtain the package by other means.
pip3 install direct_redis
