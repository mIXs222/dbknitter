#!/bin/bash

# Update package lists
apt-get update

# Ensure pip is installed
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pymongo
