#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install MongoDB driver 'pymongo'
pip3 install pymongo

# Install pandas for data manipulation
pip3 install pandas

# Install direct_redis (assuming it is available on PyPi)
pip3 install direct_redis
