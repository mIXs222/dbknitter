#!/bin/bash
# install_dependencies.sh

# Update the package index
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pymongo using pip
pip3 install pymongo
