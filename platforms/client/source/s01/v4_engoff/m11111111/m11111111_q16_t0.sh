#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 if not installed
sudo apt-get install -y python3

# Install pip for Python3 if not installed
sudo apt-get install -y python3-pip

# Install pymongo using pip
pip3 install pymongo
