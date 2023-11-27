#!/bin/bash
# Install the required dependencies

# Updating repository information
sudo apt-get update

# Install Python 3 and pip if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis direct-redis
