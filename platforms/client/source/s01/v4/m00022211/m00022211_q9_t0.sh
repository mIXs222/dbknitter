#!/bin/bash

# Update package list and install required system packages
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev libmysqlclient-dev

# Install Python packages
pip3 install pymysql pymongo pandas redis direct_redis sqlalchemy
