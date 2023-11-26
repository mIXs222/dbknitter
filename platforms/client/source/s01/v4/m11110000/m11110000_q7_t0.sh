#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and PIP if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install the Python dependencies
pip3 install pymysql pymongo
