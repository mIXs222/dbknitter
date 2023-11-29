#!/bin/bash

# Update the package index
sudo apt-get update

# Upgrade the system
sudo apt-get upgrade -y

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pymysql and pymongo
pip3 install pymysql pymongo
