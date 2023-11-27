#!/bin/bash
# Bash script to install all dependencies for the python script

# Update package list
apt-get update

# Install MongoDB dependency
apt-get install -y mongodb

# Install Redis dependency
apt-get install -y redis-server

# Install pip for Python package management
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo pandas direct_redis
