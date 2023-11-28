#!/bin/bash
# setup.sh

# Update the package list
sudo apt-get update

# Install pip if not already installed
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo pandas redis direct_redis
