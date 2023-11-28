#!/bin/bash
# Install dependencies script

# Update package list
apt-get update

# Install Python
apt-get install -y python3

# Install pip
apt-get install -y python3-pip

# Install pandas using pip
pip3 install pandas

# Install direct_redis using pip (Please replace this with the correct package installation line if the package exists, if not assume the package is direct_redis)
pip3 install direct_redis
