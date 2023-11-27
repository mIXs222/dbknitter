#!/bin/bash

# Update packages and install pip if not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymongo for MongoDB connection
pip3 install pymongo

# Install direct_redis, assuming it is available through pip (package name is hypothetical as direct_redis is not a known public Python package)
pip3 install direct_redis

# Install pandas for DataFrame operations
pip3 install pandas
