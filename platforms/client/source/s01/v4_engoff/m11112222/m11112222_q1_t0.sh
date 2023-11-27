#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and Pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install Pandas
pip3 install pandas

# Assuming direct_redis is a python package available in PyPI, install it using pip
pip3 install direct_redis
