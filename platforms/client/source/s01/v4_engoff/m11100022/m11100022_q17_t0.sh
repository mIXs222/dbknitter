#!/bin/bash

# Update package list
apt-get update

# Install Python3 and Pip if they are not installed
apt-get install -y python3 python3-pip

# Install the pymongo and direct_redis packages
pip3 install pymongo direct-redis pandas

# Note: Depending on the environment, you may need sudo or different commands to install packages.
