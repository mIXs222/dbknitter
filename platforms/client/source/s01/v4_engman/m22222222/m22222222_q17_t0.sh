#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip for Python3 if not already installed
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pandas
pip3 install direct-redis

# Note: Make sure that direct_redis is available as a package or adjust according to the real package name

