#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip if they are not available
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis direct-redis
