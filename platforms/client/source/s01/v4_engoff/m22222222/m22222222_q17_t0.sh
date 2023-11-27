#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip for Python3 if not already installed
sudo apt-get install -y python3-pip

# Alternatively, ensure pip is installed regardless of Python version
# wget -qO- https://bootstrap.pypa.io/get-pip.py | python

# Install Pandas via pip for data manipulation
pip install pandas

# Install direct_redis via pip for connecting to Redis
pip install git+https://github.com/data-axle/direct_redis.git
