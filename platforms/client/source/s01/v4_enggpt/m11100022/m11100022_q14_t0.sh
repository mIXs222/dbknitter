#!/bin/bash
# Bash Script to install all dependencies for the Python code

# Update and install pip if not already installed
sudo apt update
sudo apt install -y python3-pip

# Install pymongo for MongoDB interaction
pip install pymongo

# Install pandas for data manipulation
pip install pandas

# Install direct_redis for Redis interaction
pip install git+https://github.com/RedisLabsModules/direct_redis.git
