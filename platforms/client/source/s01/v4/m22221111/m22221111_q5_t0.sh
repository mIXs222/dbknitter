#!/bin/bash

# Update the system's package index
apt-get update

# Install Python and pip (if not already installed)
apt-get install -y python3
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo pandas redis direct-redis
