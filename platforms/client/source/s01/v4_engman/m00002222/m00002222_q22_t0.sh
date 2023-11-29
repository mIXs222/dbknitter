#!/bin/bash

# Update the package index
sudo apt update

# Install pip for Python 3 if not already installed
sudo apt install -y python3-pip

# Install pandas package using pip
pip3 install pandas

# Install direct_redis package (assuming it's available through pip, otherwise use the proper installation method provided by its maintainers)
pip3 install direct_redis
