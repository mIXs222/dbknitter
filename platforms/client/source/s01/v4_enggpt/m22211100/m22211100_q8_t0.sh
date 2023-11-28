#!/bin/bash

# Update package list
apt-get update

# Install Python3 and PIP if not already installed
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pandas pymysql pymongo direct_redis
