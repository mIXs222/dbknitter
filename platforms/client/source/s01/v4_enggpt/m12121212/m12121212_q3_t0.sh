#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python pip
sudo apt-get install -y python3-pip

# Install Python MongoDB driver pymongo
pip3 install pymongo

# Install direct_redis
pip3 install direct-redis

# Install pandas
pip3 install pandas
