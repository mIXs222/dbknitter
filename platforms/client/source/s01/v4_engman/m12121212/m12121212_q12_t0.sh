#!/bin/bash

# Update package list
apt-get update

# Install Python3 and pip if they are not already installed
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas direct_redis
