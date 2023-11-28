#!/bin/bash

# Update package list and install Python3 and pip (if not installed)
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct_redis
