#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python 3 and pip (if not already installed)
sudo apt-get install -y python3 python3-pip

# Install pandas
pip3 install pandas

# Install pymongo
pip3 install pymongo

# Install direct_redis
pip3 install direct_redis
