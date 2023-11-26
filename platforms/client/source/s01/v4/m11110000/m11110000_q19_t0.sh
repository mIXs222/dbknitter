#!/bin/bash
# Update package manager
apt-get update

# Install Python 3 and pip if not already installed
apt-get install -y python3 python3-pip

# Install pymysql and pymongo
pip3 install pymysql pymongo
