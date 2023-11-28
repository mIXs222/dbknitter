#!/bin/bash
# install_dependencies.sh

# Update the package list
sudo apt-get update

# Install pip if it's not installed
sudo apt-get install -y python3-pip

# Install pymongo
pip install pymongo
