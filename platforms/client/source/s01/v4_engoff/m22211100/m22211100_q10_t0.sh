#!/bin/bash
# Bash script to install necessary dependencies for the Python code

# Update package list and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python library dependencies using pip
pip3 install pymysql pandas pymongo redis direct_redis
