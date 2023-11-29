#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python 3 and PIP if not already installed
sudo apt-get install -y python3 python3-pip

# Install Python MySQL client library
pip3 install pymysql

# Install Python MongoDB client library
pip3 install pymongo
