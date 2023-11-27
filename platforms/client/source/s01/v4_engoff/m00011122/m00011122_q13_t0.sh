#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install pip and Python dev tools
apt-get install -y python3-pip python3-dev

# Install Python libraries
pip3 install pymongo pandas redis

# Install direct_redis from the provided location
pip3 install git+https://github.com/myusername/direct_redis.git
