#!/bin/bash

# Update package lists
apt-get update

# Install Python and Pip if they are not already installed
apt-get install -y python3 python3-pip

# Install the required Python dependencies
pip3 install pandas redis direct_redis
