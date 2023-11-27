#!/bin/bash

# Update package index
sudo apt-get update

# Install Python3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymongo (Python MongoDB driver)
pip3 install pymongo
