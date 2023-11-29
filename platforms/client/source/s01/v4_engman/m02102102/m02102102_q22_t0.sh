#!/bin/bash

# Update package list
apt-get update

# Install Python3 and PIP if not already installed
apt-get install -y python3 python3-pip

# Install PyMySQL and PyMongo using PIP
pip3 install pymysql pymongo
