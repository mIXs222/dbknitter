#!/bin/bash

# Update system
apt-get update
# Install pip (Python package installer)
apt-get install -y python3-pip
# Install pymongo using pip
pip3 install pymongo
