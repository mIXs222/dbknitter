#!/bin/bash

# Ensure Python 3 and pip is installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymysql pymongo pandas direct-redis
