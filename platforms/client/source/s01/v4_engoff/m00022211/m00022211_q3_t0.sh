#!/bin/bash

# Update repository and Upgrade system
sudo apt-get update && sudo apt-get -y upgrade

# Install Python3 and pip
sudo apt-get install -y python3 python3-pip

# Install MongoDB client (pymongo)
pip3 install pymongo

# Install Redis client (assuming 'direct_redis' can be installed via pip)
pip3 install direct_redis

# Install Pandas
pip3 install pandas
