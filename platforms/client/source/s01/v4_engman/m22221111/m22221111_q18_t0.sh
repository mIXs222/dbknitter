#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and PIP (if not already installed)
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install required Python packages using PIP
pip3 install pymongo
