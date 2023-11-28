#!/bin/bash

# Update the system's package index
sudo apt-get update

# Install Python3 and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install the pymongo and direct_redis packages using pip
pip3 install pymongo direct-redis pandas

# Install any additional missing dependencies if necessary
