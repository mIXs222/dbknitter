#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python if not already installed
sudo apt-get install -y python3

# Install pip for Python package management if not already installed
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo
