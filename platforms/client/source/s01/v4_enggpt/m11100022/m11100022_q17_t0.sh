#!/bin/bash

# Update package list and install Python3 pip if it's not available
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pandas pymongo direct_redis
