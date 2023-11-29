#!/bin/bash

# Update package list and upgrade
apt-get update && apt-get -y upgrade

# Install Python3 and pip
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pandas direct_redis

# Note: If direct_redis has any additional dependencies or is not available via pip,
# the corresponding instructions to install it will be added here.
