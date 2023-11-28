#!/bin/bash

# Update system package index
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas direct_redis msgpack-python

# Note: direct_redis may not be an actual Python package, it is assumed to be the correct package as per the instructions. 
# If direct_redis is not available, the user should check the correct package for connecting to Redis and install it.
