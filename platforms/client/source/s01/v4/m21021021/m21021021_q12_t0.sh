#!/bin/bash

# Update package list
apt-get update

# Install Python and PIP if not already installed
apt-get install -y python3 python3-pip

# Install required Python modules
pip3 install pymongo direct_redis pandas
