#!/bin/bash
# Update package lists
sudo apt-get update

# Install Python if it's not installed
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo
