#!/bin/bash

# Update the package list and install pip if it's not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo pandas redis direct-redis
