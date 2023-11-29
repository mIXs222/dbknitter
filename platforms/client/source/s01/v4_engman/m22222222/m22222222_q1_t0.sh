#!/bin/bash

# Update package list
sudo apt-get update

# Install pip for Python package management
sudo apt-get install -y python3-pip

# Install Python packages required for the script
pip3 install pandas direct_redis
