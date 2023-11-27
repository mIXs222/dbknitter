#!/bin/bash

# Update package lists
apt-get update

# Install Python 3 and pip if not already installed
apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pandas redis direct-redis
