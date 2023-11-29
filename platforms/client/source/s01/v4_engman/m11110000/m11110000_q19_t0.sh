#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install Python MySQL driver - pymysql
pip3 install pymysql

# Install Python MongoDB driver - pymongo
pip3 install pymongo

# Set permission to execute the python script
chmod +x query.py
