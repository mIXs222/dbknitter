#!/bin/bash

# This script installs the necessary dependencies for the Python script.

# Update package lists
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install required Python packages
pip3 install pymongo pandas direct-redis
