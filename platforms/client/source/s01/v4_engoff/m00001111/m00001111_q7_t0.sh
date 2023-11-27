#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install Python MySQL driver
pip3 install pymysql

# Install Python MongoDB driver
pip3 install pymongo
