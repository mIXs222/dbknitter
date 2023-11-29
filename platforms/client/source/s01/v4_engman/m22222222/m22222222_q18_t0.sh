#!/bin/bash

# Update the repository information
apt-get update

# Install Python
apt-get install -y python3 python3-pip

# Install Redis
apt-get install -y redis-server

# Install required Python packages
pip3 install pandas redis direct_redis
