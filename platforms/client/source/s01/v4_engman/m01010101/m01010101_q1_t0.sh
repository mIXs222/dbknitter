#!/bin/bash
# Update package list
apt-get update
# Install pip for Python 3
apt-get install -y python3-pip
# Install pymongo using pip
pip3 install pymongo
