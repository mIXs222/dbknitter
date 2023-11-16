#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python if not already installed
sudo apt-get install -y python3

# Install pip for Python 3 if not already installed
sudo apt-get install -y python3-pip

# Install Pandas library using pip
pip3 install pandas

# Install direct_redis library using pip
pip3 install git+https://github.com/lordralinc/direct_redis.git#egg=direct_redis
