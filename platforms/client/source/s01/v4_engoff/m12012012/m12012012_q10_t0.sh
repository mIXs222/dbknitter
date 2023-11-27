#!/bin/bash
# install_dependencies.sh

# Update package lists
apt-get update

# Install Python3 and PIP if not already installed
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo pandas direct-redis
