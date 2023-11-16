#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip for Python3 if it's not already installed
sudo apt-get install -y python3-pip

# Install pandas
pip3 install pandas

# Install the custom direct_redis module (assuming it's available through pip, this might need to be adjusted if it's a specialized module)
pip3 install direct_redis
