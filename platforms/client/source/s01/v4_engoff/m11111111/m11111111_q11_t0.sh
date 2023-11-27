#!/bin/bash

# Update package list and install python3-pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo
