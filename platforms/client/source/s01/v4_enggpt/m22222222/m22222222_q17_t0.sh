#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip for Python package management
sudo apt-get install -y python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install pandas for data manipulation
pip3 install pandas

# Install direct_redis to work with Redis
pip3 install direct_redis

# Install Python Redis client (might not be necessary due to direct_redis usage)
pip3 install redis
