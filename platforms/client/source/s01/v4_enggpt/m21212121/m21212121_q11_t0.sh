#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip and Python dev tools
sudo apt-get install -y python3-pip python3-dev

# Install the required Python packages
pip3 install pymongo pandas direct-redis
