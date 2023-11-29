#!/bin/bash
# This is install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python pip
sudo apt-get install -y python3-pip

# Install Python MongoDB driver (pymongo)
pip3 install pymongo

# Install direct_redis package
pip3 install git+https://github.com/RealKinetic/direct_redis.git

# Install pandas
pip3 install pandas
