#!/bin/bash

# Update package index
sudo apt-get update

# Install Python and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install dependencies for Python script
pip3 install pymysql pymongo pandas redis direct_redis
