#!/bin/bash

# Update the package list
apt-get update -y

# Install Python3 and pip if they are not already installed
apt-get install python3 -y
apt-get install python3-pip -y

# Ideally, you should use a virtual environment in production setups. For simplicity:
# Install the pandas library
pip3 install pandas

# Install direct_redis library (assuming it's available in PyPI)
pip3 install direct_redis
