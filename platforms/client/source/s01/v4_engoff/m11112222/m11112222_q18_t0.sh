#!/bin/bash

# Update package manager and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pandas and direct_redis packages
pip3 install pandas
pip3 install direct-redis  # Assuming that the 'direct_redis' is a package available in the repository

# Run the Python script
python3 query_data.py
