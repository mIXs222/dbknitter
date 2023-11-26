#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip (if not already installed)
apt-get install -y python3 python3-pip

# Install MongoDB driver
pip3 install pymongo

# Install Redis driver and its dependencies
pip3 install git+https://github.com/20c/direct_redis.git

# Install Pandas
pip3 install pandas
