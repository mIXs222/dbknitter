#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip for Python3 if it's not already installed
sudo apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo
