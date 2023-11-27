#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 and Python3-pip if not installed
sudo apt-get install -y python3 python3-pip

# Make sure pip is up to date
pip3 install --upgrade pip

# Install pymongo for MongoDB operations
pip3 install pymongo
