#!/bin/bash
# File: install_dependencies.sh

# Update package lists
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install pymysql and pymongo with pip
pip3 install pymysql pymongo
