#!/bin/bash
# Bash script to install dependencies for the Python script

# Update package list
sudo apt-get update

# Install pip if it's not already installed
sudo apt-get install -y python3-pip

# Install pymongo to interact with MongoDB
pip3 install pymongo
