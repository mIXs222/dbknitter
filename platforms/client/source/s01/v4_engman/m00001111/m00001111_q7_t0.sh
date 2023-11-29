#!/bin/bash
# install_dependencies.sh

# Update package lists
apt-get update

# Install Python and Pip if they are not already installed
apt-get install -y python3 python3-pip

# Install pymysql and pymongo Python packages
pip3 install pymysql pymongo
