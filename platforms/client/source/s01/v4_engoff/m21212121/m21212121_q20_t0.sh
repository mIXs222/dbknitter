#!/bin/bash
# Update package list
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo redis pandas direct_redis
