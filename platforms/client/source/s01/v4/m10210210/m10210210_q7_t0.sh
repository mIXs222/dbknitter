#!/bin/bash
# Bash script to install all dependencies

# Update package lists
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pandas pymysql pymongo direct_redis
