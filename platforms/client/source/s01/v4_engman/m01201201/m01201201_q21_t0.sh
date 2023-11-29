#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install MySQL client
sudo apt-get install -y default-mysql-client

# Install pip for Python 3 if it's not available
sudo apt-get install -y python3-pip

# Install Python MySQL driver
pip3 install pymysql

# Install Python MongoDB driver
pip3 install pymongo
