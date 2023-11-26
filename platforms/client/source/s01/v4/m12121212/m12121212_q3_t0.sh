#!/bin/bash
# Install Python dependencies for the Python script

# Update repositories and install pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo pandas redis direct_redis
