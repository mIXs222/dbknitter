#!/bin/bash

# install_dependencies.sh

# Update package lists
apt-get update

# Install Python3 and Pip
apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install pymysql, pymongo, direct_redis and pandas
pip3 install pymysql pymongo direct_redis pandas
