#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 and pip (if not installed)
sudo apt-get install -y python3 python3-pip

# Install Redis and pandas via pip
pip3 install redis pandas direct_redis
