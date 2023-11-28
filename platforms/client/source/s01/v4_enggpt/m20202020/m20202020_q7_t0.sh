#!/bin/bash
# File: setup_dependencies.sh

# Update package list
sudo apt-get update

# Install Python3 pip if not already installed
sudo apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql

# Install pandas
pip3 install pandas

# Install redis-py, assuming direct_redis is a third-party package 
# Note: If direct_redis is not available in PyPI, ensure you have access to this specific package.
pip3 install redis direct_redis
