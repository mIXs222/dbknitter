#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and pip if not installed
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pandas pymysql pymongo direct_redis
