#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python 3 and pip
sudo apt-get install -y python3 python3-pip

# Install Pandas via pip
pip3 install pandas

# Install direct_redis via pip (assuming it is available in the pip repository)
pip3 install direct_redis
