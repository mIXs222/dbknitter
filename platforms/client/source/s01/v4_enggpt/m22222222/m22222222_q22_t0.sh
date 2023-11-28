#!/bin/bash

# Update package lists
apt-get update

# Install Python3 and pip
apt-get install -y python3 python3-pip

# Install pandas and redis-py via pip
pip3 install pandas redis
