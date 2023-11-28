#!/bin/bash
# install_dependencies.sh

# Updating package lists
sudo apt-get update

# Installing Python3 and pip3
sudo apt-get install -y python3 python3-pip

# Installing MySQL client
sudo apt-get install -y default-libmysqlclient-dev

# Installing MongoDB
sudo apt-get install -y mongodb-clients

# Installing Redis tools
sudo apt-get install -y redis-tools

# Creating a virtual environment and activating it
python3 -m venv venv
source venv/bin/activate

# Installing Python package dependencies
pip3 install pymysql pymongo pandas direct_redis

# Deactivating the virtual environment
deactivate
