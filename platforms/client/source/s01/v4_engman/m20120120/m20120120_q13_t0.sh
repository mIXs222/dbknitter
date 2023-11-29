#!/bin/bash
# Bash script to install dependencies (install_dependencies.sh)

# Update package lists
apt-get update

# Install Python and pip if they are not already installed
apt-get install -y python3 python3-pip

# Install Python MongoDB client library, pymonogo
pip3 install pymongo

# Install pandas for DataFrame support in Python
pip3 install pandas

# Install direct_redis library to interact with Redis
pip3 install git+https://github.com/coleifer/direct_redis
