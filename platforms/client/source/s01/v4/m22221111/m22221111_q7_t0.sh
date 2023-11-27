#!/bin/bash

# Update package lists
sudo apt-get update

# Install required system packages
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas redis

# Install direct_redis with pandas support through pip directly from its repository
pip3 install git+https://github.com/20c/direct_redis.git#egg=direct_redis[pandas]
