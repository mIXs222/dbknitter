#!/bin/bash

# This script assumes you are using a Debian/Ubuntu system

# Update package lists
sudo apt-get update

# Python3 and PIP (if they aren't already installed)
sudo apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql

# Install PyMongo
pip3 install pymongo
