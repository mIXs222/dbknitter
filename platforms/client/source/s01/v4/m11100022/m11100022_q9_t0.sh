#!/bin/bash
# install_dependencies.sh

# Update repositories and install Python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct-redis
