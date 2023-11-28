#!/bin/bash
# This script assumes you are using a Debian-based system (like Ubuntu)

# Update system package index
sudo apt-get update

# Install Python 3
sudo apt-get install -y python3

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pymongo using pip
pip3 install pymongo
