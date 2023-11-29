#!/bin/bash

# Ensure that we have the latest package lists
apt-get update

# Install Python3 and the pip package manager
apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql

# Install PyMongo
pip3 install pymongo
