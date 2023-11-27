#!/bin/bash
# You may need to run this script with superuser privileges.

# Update package index
sudo apt-get update

# Install Python and pip, if not already installed
sudo apt-get install -y python3 python3-pip

# Install pandas and direct_redis
pip3 install pandas direct-redis
