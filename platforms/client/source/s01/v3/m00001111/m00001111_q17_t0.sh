#!/bin/sh

# Ensure Python 3 is installed
sudo apt-get update
sudo apt-get install python3.6

# Ensure pip is installed
sudo apt-get install -y python3-pip

# Install necessary Python packages
pip3 install mysql-connector-python
pip3 install pymongo
