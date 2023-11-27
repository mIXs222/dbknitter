#!/bin/bash
# Bash script to install dependencies for running the Python script

# Update package list
sudo apt update

# Install pip for Python package management
sudo apt install -y python3-pip

# Install MongoDB driver pymongo
pip3 install pymongo

# Install Direct Redis and Pandas
pip3 install direct-redis pandas
