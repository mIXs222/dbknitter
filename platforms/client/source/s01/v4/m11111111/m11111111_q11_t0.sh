#!/bin/bash
# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python3 and pip if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymongo with pip3
pip3 install pymongo
