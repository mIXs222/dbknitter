#!/bin/bash

# Update package lists
apt-get update

# Install pip if it's not installed
apt-get install -y python3-pip

# Upgrade pip
pip3 install --upgrade pip

# Install the required Python packages
pip3 install pandas pymongo redis direct-redis

# Ensure the script has execute permissions
chmod +x install_dependencies.sh
