#!/bin/bash

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install necessary Python libraries
pip3 install pymysql pymongo pandas direct_redis

# Clean up cache to free up space
apt-get clean
