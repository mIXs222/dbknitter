#!/bin/bash

# install_dependencies.sh

# Update package lists
apt-get update

# Install MySQL client and libraries
apt-get install -y default-mysql-client default-libmysqlclient-dev

# Install MongoDB client
apt-get install -y mongodb-clients

# Install Python3 and pip if not already available
apt-get install -y python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install required Python libraries
pip3 install pymysql pymongo
