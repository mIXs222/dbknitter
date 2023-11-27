#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install Python3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install required packages
pip3 install pymysql pymongo
