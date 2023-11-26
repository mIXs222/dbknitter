#!/bin/bash
# This script is used to install dependencies for running the provided Python script.

# Update package lists
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install MongoDB driver for Python
pip3 install pymongo
