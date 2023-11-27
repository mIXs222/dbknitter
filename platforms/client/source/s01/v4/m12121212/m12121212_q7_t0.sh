#!/bin/bash

# install_dependencies.sh

# Update the package list
sudo apt-get update

# Install pip for Python3 if not already installed
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo pandas direct_redis
