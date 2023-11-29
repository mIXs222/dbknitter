#!/bin/bash

# Update the package index
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql

# Install PyMongo
pip3 install pymongo
