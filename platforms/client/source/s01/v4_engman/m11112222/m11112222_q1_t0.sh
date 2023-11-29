#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python 3 and pip (if not already installed)
sudo apt-get install -y python3 python3-pip

# Install pandas and direct-redis via pip
pip3 install pandas direct_redis
