#!/bin/bash

# Update package manager
sudo apt-get update

# Install pip if it's not already installed
sudo apt-get install -y python3-pip

# Ensure pymongo, pandas, and direct_redis clients are installed for python3
pip3 install pymongo pandas

# Since 'direct_redis' is not a known package, this is a placeholder
# Replace 'direct_redis_package' with the correct package name if it exists
# pip3 install direct_redis_package

echo "Dependencies for Python script installed."
