#!/bin/bash
# Update the package list
apt-get update

# Install Python Pip for Python3
apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql

# Install PyMongo
pip3 install pymongo
