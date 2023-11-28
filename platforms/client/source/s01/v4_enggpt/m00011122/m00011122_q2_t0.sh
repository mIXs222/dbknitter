#!/bin/bash

# Update package lists
sudo apt-get update

# Install MySQL client
sudo apt-get install -y default-mysql-client

# Install MongoDB client
sudo apt-get install -y mongodb-clients

# Install pip for Python3 if not already installed
sudo apt-get install -y python3-pip

# Install Python libraries required for the Python script
pip3 install pymysql pymongo
