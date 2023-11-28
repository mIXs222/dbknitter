#!/bin/bash
# install_dependencies.sh

# Update package lists
apt-get update

# Install python3 and pip if they are not already installed
apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install pymysql, pymongo, pandas, and direct_redis
pip3 install pymysql pymongo pandas git+https://github.com/javatechy/direct_redis.git
