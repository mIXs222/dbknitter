#!/bin/bash
# Assuming Debian-based system

# Update apt repositories
sudo apt update

# Install Python and pip
sudo apt install -y python3 python3-pip

# Install MongoDB driver
pip3 install pymongo

# Install direct_redis (assuming it is a publicly available package)
pip3 install direct_redis

# Install pandas (for DataFrame operations)
pip3 install pandas
