#!/bin/bash
# This script is used to install Python dependencies for running the Python script

# Update package list
apt-get update

# Install pip for Python 3
apt-get install -y python3-pip

# Install pymongo using pip
pip3 install pymongo
