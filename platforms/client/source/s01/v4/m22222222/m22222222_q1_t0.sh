#!/bin/bash

# Update package list
sudo apt-get update

# Install pip if not installed
sudo apt-get install -y python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install pandas
pip3 install pandas

# Install the package required for direct_redis (e.g. redis-py)
pip3 install redis

# Assuming that direct_redis is a custom package, install it using the provided method
# Here it's a placeholder since direct_redis is not a standard package available in PyPi
# Replace with actual installation process if available
pip3 install direct_redis
