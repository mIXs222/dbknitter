#!/bin/bash
# File: install_dependencies.sh

# Update package list
apt-get update

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas redis direct_redis
