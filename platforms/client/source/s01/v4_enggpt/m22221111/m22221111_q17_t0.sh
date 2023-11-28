#!/bin/bash

# Update package lists
apt-get update

# Install Python 3 and pip
apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install direct_redis
pip3 install direct_redis
