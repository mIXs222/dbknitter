#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install pip
apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo pandas direct-redis
