#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and Pip (if not already installed)
sudo apt-get install -y python3 python3-pip

# Install MongoDB driver for Python
pip3 install pymongo

# Install Redis driver for Python
pip3 install git+https://github.com/yahoo/direct_redis.git

# Install Pandas
pip3 install pandas
