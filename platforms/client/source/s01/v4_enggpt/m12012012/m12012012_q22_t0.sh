#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3, PIP and MongoDB
sudo apt-get install -y python3 python3-pip mongodb

# Install Python dependencies
pip3 install pymysql pymongo
