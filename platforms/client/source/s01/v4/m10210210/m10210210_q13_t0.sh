#!/bin/bash

# Update system packages and install python-pip and necessary system tools
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python packages
pip3 install pymongo pandas redis direct-redis
