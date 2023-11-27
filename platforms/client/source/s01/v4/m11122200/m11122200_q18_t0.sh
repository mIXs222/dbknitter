#!/bin/bash
# This script installs all required dependencies to run the Python code for the given query

# Update package lists
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install pymysql, pandas, and direct_redis
pip3 install pymysql pandas direct_redis
