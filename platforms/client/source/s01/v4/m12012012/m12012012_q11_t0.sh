#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip and Python development files
sudo apt-get install -y python3-pip python3-dev

# Install required Python packages
pip3 install pymongo redis pandas
