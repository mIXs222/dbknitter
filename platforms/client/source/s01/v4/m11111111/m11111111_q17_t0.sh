#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip3 if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymongo MongoDB driver for Python
pip3 install pymongo

echo "All dependencies have been installed."
