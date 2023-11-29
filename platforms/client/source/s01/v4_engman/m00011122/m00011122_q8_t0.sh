#!/bin/bash

# install_dependencies.sh

# Update package list
apt-get update

# Install Python3 and pip
apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pymongo pandas direct_redis
