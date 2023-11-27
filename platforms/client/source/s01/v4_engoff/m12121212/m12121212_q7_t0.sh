#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip
apt-get install -y python3
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymongo pandas redis direct_redis
