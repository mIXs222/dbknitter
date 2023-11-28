#!/bin/bash

# Update package list and upgrade existing packages
apt-get update
apt-get upgrade -y

# Install pip, Python's package manager
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymongo pandas redis direct_redis
