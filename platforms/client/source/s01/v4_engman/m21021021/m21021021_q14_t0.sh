#!/bin/bash

# install_dependencies.sh

# Update package lists
apt-get update

# Install Python 3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install the pymysql and pymongo Python packages
pip3 install pymysql pymongo
