#!/bin/bash
set -e

# Update package lists
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql

# Install PyMongo
pip3 install pymongo
