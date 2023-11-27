#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt update

# Install Python3 and PIP if they're not installed
sudo apt install -y python3 python3-pip

# Install the Python libraries pymysql and pymongo
pip3 install pymysql pymongo
