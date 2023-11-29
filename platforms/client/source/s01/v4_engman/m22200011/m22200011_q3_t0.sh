#!/bin/bash

# Update the package index
sudo apt-get update

# Install Python3 pip
sudo apt-get install -y python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
