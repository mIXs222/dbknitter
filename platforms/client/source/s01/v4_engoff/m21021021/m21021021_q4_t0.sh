#!/bin/bash

# Update the package index
sudo apt-get update

# Install Python 3 and pip
sudo apt-get install -y python3 python3-pip

# Install MongoDB driver (pymongo)
pip3 install pymongo

# Install direct_redis package
pip3 install direct-redis

# Install pandas package
pip3 install pandas
