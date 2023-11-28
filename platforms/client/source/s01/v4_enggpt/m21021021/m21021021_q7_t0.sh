#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python3 and pip
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pymongo pandas direct_redis
