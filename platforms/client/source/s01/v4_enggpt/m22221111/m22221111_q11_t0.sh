#!/bin/bash

# Update package list and upgrade packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis direct-redis

# Note: Ensure that `direct-redis` package is available in the pip repository or adjust accordingly
