#!/bin/bash

# Update package list and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the Python libraries
pip3 install pymysql pymongo pandas 

# Assuming direct_redis is a Python package available in the repository:
pip3 install direct_redis
