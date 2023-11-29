#!/bin/bash
# Update package list
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo

# Run the query Python script
python3 mongodb_query.py
