#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python3 pip
sudo apt-get install -y python3-pip

# Install MySQL client
sudo apt-get install -y default-mysql-client

# Install MongoDB client
sudo apt-get install -y mongodb-clients

# Install pymysql
pip3 install pymysql

# Install pymongo
pip3 install pymongo
