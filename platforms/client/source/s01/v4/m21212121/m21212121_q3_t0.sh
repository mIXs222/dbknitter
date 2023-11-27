#!/bin/bash
# This script installs dependencies required to run the Python script

# Update package list and install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo redis direct_redis pandas
