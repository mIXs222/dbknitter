#!/bin/bash
# install_dependencies.sh

# Update the package lists
sudo apt-get update

# Install Python3 and pip3 if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo

# Install redis-py
pip3 install redis

# Install pandas
pip3 install pandas
