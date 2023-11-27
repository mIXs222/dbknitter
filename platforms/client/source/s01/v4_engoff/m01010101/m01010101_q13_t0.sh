#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and PIP if they're not already installed
sudo apt-get install -y python3 python3-pip

# Install the Python MySQL client
pip3 install pymysql

# Install the Python MongoDB client
pip3 install pymongo
