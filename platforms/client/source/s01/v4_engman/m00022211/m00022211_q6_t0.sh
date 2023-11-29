#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and PIP if they are not installed
sudo apt-get install -y python3 python3-pip

# Install the pymongo package
pip3 install pymongo
