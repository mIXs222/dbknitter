#!/bin/bash

# Update package list
apt-get update

# Install Python3 and pip if not available
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas direct_redis
