#!/bin/bash

# Update the package list
apt-get update

# Install pip if it's not already installed
apt-get install -y python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install the required Python packages
pip3 install pymongo pandas direct-redis
