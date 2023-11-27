#!/bin/bash

# Update repositories and install Python3
sudo apt-get update
sudo apt-get install -y python3

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install pymysql and pymongo Python packages
pip3 install pymysql pymongo
