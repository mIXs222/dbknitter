#!/bin/bash

# Activate virtual environment if needed
# source /your/virtualenv/bin/activate

# Update package manager and install Python3 and pip if they are not installed
sudo apt update
sudo apt install -y python3
sudo apt install -y python3-pip

# Install the Python packages required for the script
pip3 install pymongo pandas redis

# Install direct_redis via pip3, assuming it is available in the Python Package Index or a similar repository
pip3 install direct_redis
