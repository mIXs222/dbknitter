#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python3, PIP and Redis if not already installed
sudo apt-get install -y python3 python3-pip redis

# Install required Python libraries
pip3 install pandas pymysql pymongo direct_redis
