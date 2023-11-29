#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pymongo pandas redis

# Install direct_redis from the given repository (example, as DirectRedis might not be available via pip directly)
pip3 install git+https://github.com/yourrepo/direct_redis.git
