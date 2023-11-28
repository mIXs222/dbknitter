#!/bin/bash

# Update package list
sudo apt-get update

# Make sure pip is installed
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo direct_redis pandas
