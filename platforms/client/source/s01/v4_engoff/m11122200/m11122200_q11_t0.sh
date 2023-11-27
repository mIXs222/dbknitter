#!/bin/bash
# Install dependencies for running the Python script

# Update repositories and prepare the system
sudo apt update

# Install python3 and python3-pip if not already installed
sudo apt install -y python3 python3-pip

# Install Python MongoDB client
pip3 install pymongo

# Install Redis client for Python
pip3 install direct-redis

# Install pandas
pip3 install pandas
