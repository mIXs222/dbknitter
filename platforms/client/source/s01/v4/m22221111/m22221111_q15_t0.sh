#!/bin/bash

# Update package listings
apt-get update

# Install Python and Pip if they are not installed
apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis direct_redis
