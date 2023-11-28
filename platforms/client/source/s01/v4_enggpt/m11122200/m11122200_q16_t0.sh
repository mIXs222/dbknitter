#!/bin/bash

# This script is for installing dependencies
# Update package index
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis direct_redis
