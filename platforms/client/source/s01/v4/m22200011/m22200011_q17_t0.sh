#!/bin/bash

# Update package list and install Python pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymongo pandas redis "direct_redis>=0.3" 

echo "Dependencies installed."
