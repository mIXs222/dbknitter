#!/bin/bash

# Ensure script is run with superuser privileges
if [ "$(id -u)" != "0" ]; then
   echo "This script must be run as root" 
   exit 1
fi

# Update package list
apt-get update

# Install Python3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Ensure pip is up to date
pip3 install --upgrade pip

# Install the required Python packages
pip3 install pymysql pymongo pandas redis direct_redis

# Note: The package 'direct_redis' should be previously available,
# since it's not a standard package and the provided instruction
# uses 'direct_redis.DirectRedis' which suggests a specialized/custom library.
