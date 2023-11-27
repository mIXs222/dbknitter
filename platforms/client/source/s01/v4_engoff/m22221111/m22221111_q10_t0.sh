#!/bin/bash

# Update the package list
apt-get update

# Install Python3 and pip if they are not already installed
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas
# Install direct_redis (assuming this custom package can be installed via pip or similar)
pip3 install direct_redis
