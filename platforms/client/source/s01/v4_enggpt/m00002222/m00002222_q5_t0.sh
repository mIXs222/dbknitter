#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip for Python3 if not already installed
sudo apt-get install -y python3-pip

# Install the pymysql library
pip3 install pymysql

# Install pandas
pip3 install pandas

# Install direct_redis library (assuming pip will fetch it from the correct source since it's not a common package)
pip3 install direct_redis
