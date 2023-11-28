#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and Pip (if not already installed)
sudo apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo
