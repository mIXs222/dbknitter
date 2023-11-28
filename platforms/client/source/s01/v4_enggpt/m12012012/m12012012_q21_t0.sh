#!/bin/bash
# This script is intended to set up the environment to run the provided Python code.

# Update package lists
sudo apt-get update

# Install Python3 and Python3-pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install MongoDB and Redis drivers for Python
pip3 install pymongo direct_redis pandas
