#!/bin/bash

# Update package index
sudo apt-get update

# Install Python3 and Python3-pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymongo using pip
pip3 install pymongo
