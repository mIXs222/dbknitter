#!/bin/bash

# install_dependencies.sh

# Update package lists
apt-get update

# Install Python pip
apt-get install -y python3-pip

# Install MongoDB driver (pymongo)
pip3 install pymongo

# Install Redis client for Python
pip3 install redis

# Install Pandas
pip3 install pandas
