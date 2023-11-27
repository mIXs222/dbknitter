#!/bin/bash

# Update package list
apt-get update

# Install pip if it is not already installed
apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis direct-redis
