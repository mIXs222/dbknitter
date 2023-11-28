#!/bin/bash

# Update package list
apt-get update

# Install Python and Pip
apt-get install -y python3
apt-get install -y python3-pip

# Install Python packages
pip3 install pymongo pandas redis direct-redis
