#!/bin/bash

# Update the package lists
apt-get update

# Install Python3 and Pip
apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis
