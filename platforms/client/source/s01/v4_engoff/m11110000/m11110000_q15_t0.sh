#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install pip and Python development files (if not already installed)
sudo apt-get install -y python3-pip python3-dev

# Install Python library dependencies
pip3 install pymysql pymongo
