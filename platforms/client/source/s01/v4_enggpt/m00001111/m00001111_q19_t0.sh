#!/bin/bash
# bash script: install_dependencies.sh

# Update package lists
apt-get update

# Upgrade the system
apt-get upgrade -y

# Install pip for Python 3
apt-get install -y python3-pip

# Install pymysql and pymongo
pip3 install pymysql pymongo
