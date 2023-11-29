#!/bin/bash

# Update the package lists
sudo apt-get update

# Install pip for Python3 if not already installed
sudo apt-get install -y python3-pip

# Install pymongo using pip
pip3 install pymongo
