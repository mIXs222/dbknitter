#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip for Python 3 (If not already installed)
sudo apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymysql pymongo
