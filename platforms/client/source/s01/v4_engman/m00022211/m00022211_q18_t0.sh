#!/bin/bash

# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install required system dependencies
sudo apt-get install -y python3-pip python3-dev build-essential

# Install Python packages
pip3 install pymongo direct-redis pandas
