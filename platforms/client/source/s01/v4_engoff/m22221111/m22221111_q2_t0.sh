#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python 3 and Pip (if not already installed)
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo direct-redis pandas
