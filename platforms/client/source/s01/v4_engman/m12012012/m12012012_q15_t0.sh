#!/bin/bash

# Update package index
sudo apt update

# Install Python 3 and pip (if not already installed)
sudo apt install -y python3 python3-pip

# Install pymongo
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install direct_redis (assuming it is a third-party package,
# if it's not available in the Python Package Index (PyPI), 
# this step would be different depending on how it's provided.)
pip3 install direct-redis

# Assuming direct_redis is not a standard package and not available
# in pip, the following is a placeholder for the actual command
# that would be needed.
# For example:
# pip3 install git+https://github.com/your-repo/direct_redis.git
