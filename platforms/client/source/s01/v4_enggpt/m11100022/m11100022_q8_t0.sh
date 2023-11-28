#!/bin/bash

# Update package index
sudo apt-get update

# Install Python 3 and PIP
sudo apt-get install python3 python3-pip -y

# Install required Python packages
pip3 install pymysql pymongo pandas redis direct_redis

# Note: The package 'direct_redis' might not exist in the PyPI repository. If not, the proper package to use
# should be 'redis'. Adjust the code and the dependency installation accordingly.
