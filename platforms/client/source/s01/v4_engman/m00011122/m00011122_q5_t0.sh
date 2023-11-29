#!/bin/bash
# install_dependencies.sh

# Update the package list
sudo apt-get update

# Install Python 3 and PIP
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct-redis
