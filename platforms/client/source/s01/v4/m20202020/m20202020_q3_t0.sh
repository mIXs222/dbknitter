#!/bin/bash
# Update package list
sudo apt-get update

# Install pip3 if not already installed
sudo apt-get install -y python3-pip

# Install pymysql - Python MySQL client library
pip3 install pymysql

# Install pandas - Python Data Analysis Library
pip3 install pandas

# Install direct_redis - potentially you may need to implement or install from a source if it's not available via pip
pip3 install direct_redis
