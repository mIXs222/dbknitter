#!/bin/bash

# Update the package index
sudo apt-get update

# Install pip for Python 3 if not already installed
sudo apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo
