#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip if not installed
sudo apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pandas pymongo direct_redis
