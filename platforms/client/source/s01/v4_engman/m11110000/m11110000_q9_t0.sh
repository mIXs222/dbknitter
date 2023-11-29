#!/bin/bash

# Update package list
sudo apt-get update

# Install pip, a package manager for Python
sudo apt-get install -y python3-pip

# Install pymysql and pymongo Python libraries
pip3 install pymysql pymongo
