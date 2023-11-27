#!/bin/bash

# Update system package index
apt-get update

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas redis direct-redis
