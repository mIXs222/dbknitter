#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and PIP
sudo apt-get install -y python3 python3-pip

# Install PyMySQL and PyMongo packages
pip3 install pymysql pymongo
