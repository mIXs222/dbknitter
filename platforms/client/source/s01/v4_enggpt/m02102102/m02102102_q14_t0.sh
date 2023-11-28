#!/bin/bash

# Update the package index
sudo apt-get update

# Install pip if it's not already installed
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pandas pymongo direct_redis
