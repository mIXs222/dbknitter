#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 and pip (if not installed)
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymongo for MongoDB connection
pip3 install pymongo
