#!/bin/bash

# Update package list and install python3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pandas and direct-redis via pip (replace 'direct-redis' if a real PyPI equivalent exists)
pip3 install pandas direct_redis
