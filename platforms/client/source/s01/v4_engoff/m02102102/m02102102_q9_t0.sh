#!/bin/bash

# Update package lists
echo "Updating package lists..."
apt-get update

# Install Python and PIP
echo "Installing Python and PIP..."
apt-get install -y python3 python3-pip

# Install Python packages
echo "Installing Python packages..."
pip3 install pymysql pymongo redis pandas

# Install direct_redis from GitHub (assuming no package is available on PyPI)
echo "Installing direct_redis from GitHub..."
pip3 install git+https://github.com/20c/direct_redis.git

echo "All dependencies are installed."
