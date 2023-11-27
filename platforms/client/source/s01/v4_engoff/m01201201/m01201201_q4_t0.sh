#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python and PIP (if not installed)
sudo apt-get install -y python3 python3-pip

# Install PyMySQL and PyMongo using PIP
pip3 install pymysql pymongo
