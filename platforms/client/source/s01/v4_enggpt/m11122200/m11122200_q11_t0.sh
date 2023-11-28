#!/bin/bash

# Update package list and install Python dependencies
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo pandas direct_redis
