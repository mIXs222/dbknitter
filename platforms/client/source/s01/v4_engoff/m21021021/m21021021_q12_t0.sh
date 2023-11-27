#!/bin/bash

# Update package lists
apt-get update

# Install Python and Pip if they aren't already installed
apt-get install -y python3 python3-pip

# Install MongoDB driver and Redis client for Python
pip3 install pymongo direct_redis pandas
