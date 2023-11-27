#!/bin/bash

# Update package list
apt-get update

# Install Python 3 pip if not already installed
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo direct_redis pandas
