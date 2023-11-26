#!/bin/bash
# Filename: install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python3, pip, and necessary system libraries
sudo apt-get install -y python3 python3-pip python3-dev build-essential

# Install pymysql via pip
pip3 install pymysql

# Install pandas via pip
pip3 install pandas

# Install redis and direct_redis via pip
pip3 install redis direct_redis
