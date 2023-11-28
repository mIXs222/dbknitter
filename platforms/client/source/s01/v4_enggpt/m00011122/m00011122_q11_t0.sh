#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and PIP (if not already installed)
sudo apt-get install -y python3 python3-pip

# Install Python MySQL driver
pip3 install pymysql

# Install Python MongoDB driver
pip3 install pymongo
