#!/bin/bash

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis

# Install git to clone repository (if needed)
apt-get install -y git

# Clone the direct_redis repository
# git clone https://github.com/agronholm/direct_redis

# Install the direct_redis package using pip
# cd direct_redis
# pip3 install .
