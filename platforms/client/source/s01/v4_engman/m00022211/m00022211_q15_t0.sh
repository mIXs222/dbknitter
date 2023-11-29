#!/bin/bash
# setup.sh

# Update the package list
sudo apt-get update

# Install pip, a package manager for Python
sudo apt-get install -y python3-pip

# Install necessary Python packages
pip3 install pymongo pandas direct_redis
