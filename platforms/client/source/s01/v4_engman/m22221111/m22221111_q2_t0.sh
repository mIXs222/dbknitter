#!/bin/bash

# Update package lists
apt-get update

# Install pip if it's not installed
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo pandas redis direct-redis
