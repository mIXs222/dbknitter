#!/bin/bash

# Bash script to set up the Python environment for the query execution

# Update the package list and install Python pip
apt-get update
apt-get install -y python3-pip

# Upgrade pip
pip3 install --upgrade pip

# Install required Python packages
pip3 install pymongo pandas redis

# Install DirectRedis (assuming DirectRedis is available as Python package
# If there is any specific command to install DirectRedis, replace the following line with the correct one
pip3 install direct-redis

# Check if the packages are successfully installed
pip3 list
