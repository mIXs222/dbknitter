#!/bin/bash

# Update the package list
apt-get update

# Install Python and pip (if not installed)
apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis datetime direct_redis
