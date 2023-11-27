#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and PIP
sudo apt-get install -y python3 python3-pip

# Install MongoDB driver for Python
pip3 install pymongo
