#!/bin/bash

# First make sure you have Python and pip installed
# You may use your package manager to install pymongo if Python and pip are already present 

# Update the package list
sudo apt-get update

# Install Python and pip (if not already installed)
sudo apt-get install -y python3 python3-pip

# Install pymongo (MongoDB driver for Python)
pip3 install pymongo
