#!/bin/bash
# File name: install_dependencies.sh

# Update package list
apt-get update

# Install Python 3 and pip
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas sqlalchemy
