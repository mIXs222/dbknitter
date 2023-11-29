#!/bin/bash

# Update package listing
apt-get update

# Install Python3 and PIP if they are not already installed
apt-get install -y python3 python3-pip

# Install pymongo using PIP
pip3 install pymongo
