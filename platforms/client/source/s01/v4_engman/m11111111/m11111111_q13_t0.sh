#!/bin/bash

# Update package list
apt-get update

# Install Python 3 and pip (if not already installed)
apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo
