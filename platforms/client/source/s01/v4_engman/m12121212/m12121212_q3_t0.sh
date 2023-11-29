#!/bin/bash

# Update package lists
sudo apt-get update -y

# Install pip for Python package management
sudo apt-get install python3-pip -y

# Install required Python packages
pip3 install pymongo direct_redis pandas
