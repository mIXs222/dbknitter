#!/bin/bash
# setup.sh

# Update package lists
apt-get update

# Install Python 3 and pip if they are not installed
apt-get install -y python3
apt-get install -y python3-pip

# Install Python packages
pip3 install pymysql
pip3 install pymongo
