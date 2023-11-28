#!/bin/bash

# Update package list
apt-get update

# Install Python and pip if needed
apt-get install -y python3
apt-get install -y python3-pip

# Install pymongo if not already installed
pip3 install pymongo
