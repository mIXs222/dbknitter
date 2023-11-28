#!/bin/bash

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymongo pandas redis direct-redis
