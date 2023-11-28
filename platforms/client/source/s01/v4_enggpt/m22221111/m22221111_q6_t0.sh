#!/bin/bash
# install_dependencies.sh

# Updating package lists
sudo apt-get update

# Install pip for Python 3, MongoDB and required libraries
sudo apt-get install -y python3-pip
sudo pip3 install pymongo
