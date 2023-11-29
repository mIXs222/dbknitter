#!/bin/bash
# Bash script saved as install_dependencies.sh

# Update package lists
sudo apt-get update

# Install pip for Python package management, if not already installed
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo direct_redis pandas
