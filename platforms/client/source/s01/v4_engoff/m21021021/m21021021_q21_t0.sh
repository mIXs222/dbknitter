#!/bin/bash
# install.sh

# Update package list
sudo apt-get update

# Install pip if it is not already installed
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymongo pandas redis

# Install DirectRedis from its repository as it might not be available on PyPI
pip3 install git+https://github.com/hangxie/direct_redis.git
