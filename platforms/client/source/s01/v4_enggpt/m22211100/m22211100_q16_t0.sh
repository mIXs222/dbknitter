#!/bin/bash
# Update package lists
apt-get update

# Install pip for python package management
apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymongo
pip3 install direct_redis
pip3 install pandas
