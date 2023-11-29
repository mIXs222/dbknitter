#!/bin/bash

# Update the package list
apt-get update

# Install pip for Python3
apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis direct_redis
