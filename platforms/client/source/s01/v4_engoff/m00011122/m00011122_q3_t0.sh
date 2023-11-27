#!/bin/bash
# filename: install_dependencies.sh

# Make sure pip is installed
sudo apt-get install python3-pip -y

# Install pymongo for MongoDB connection
pip3 install pymongo

# Install pandas for data manipulation
pip3 install pandas

# Install direct_redis since it is mentioned instead of regular Redis client
pip3 install direct-redis
