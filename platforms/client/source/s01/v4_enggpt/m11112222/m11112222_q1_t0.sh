#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and Pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas direct-redis
