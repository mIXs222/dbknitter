#!/bin/bash
# install.sh

# Update package list and install Python3 and pip if not already installed
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymysql and pymongo
pip3 install pymysql
pip3 install pymongo

# Run the Python script
python3 query.py
