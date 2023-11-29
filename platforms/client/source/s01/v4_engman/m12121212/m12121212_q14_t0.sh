#!/bin/bash

# Update package lists
apt-get update

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas redis direct_redis
