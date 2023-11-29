#!/bin/bash

# Update package list
apt-get update

# Install Python and PIP
apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymongo direct-redis pandas
