#!/bin/bash

# install_dependencies.sh

# Update package list
sudo apt-get update

# Install pip for Python 3 if not already installed
sudo apt-get install -y python3-pip

# Install Python MongoDB driver
pip3 install pymongo

# Install Python Redis client
pip3 install direct_redis

# Install pandas
pip3 install pandas
