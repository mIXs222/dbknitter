#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 and pip if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo pandas redis direct_redis
