#!/bin/bash
# install_dependencies.sh

# Update package list and install pip for Python3 if it's not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct-redis
