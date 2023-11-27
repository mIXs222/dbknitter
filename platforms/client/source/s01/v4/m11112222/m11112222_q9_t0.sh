#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install MongoDB
sudo apt-get install -y mongodb

# Install Redis
sudo apt-get install -y redis-server

# Install pymongo
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install the direct_redis package (assuming it’s available. If this custom package is not available on PyPI, 
# you need to provide an alternative installation method such as from a Git repo or local directory)
pip3 install git+https://github.com/your_username/direct_redis.git   # Replace with actual URL if public.

# Upgrade setuptools if needed
pip3 install --upgrade setuptools

# NOTE: The dependencies for SQLite should already be included in Python3 as the 'sqlite3' module is in the standard library.
# In case you are using a custom SQLite module that needs installation, use pip or your package manager to install it.
