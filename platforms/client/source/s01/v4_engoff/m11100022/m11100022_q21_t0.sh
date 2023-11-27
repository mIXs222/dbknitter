#!/bin/bash

# Update package list
apt-get update

# Install dependencies
apt-get install -y python3-pip
pip3 install pandas pymysql pymongo redis direct-redis

# Note: The package 'direct-redis' does not exist in the Python Package Index.
# It seems to be a custom package. You should install it manually or provide the correct package source.
