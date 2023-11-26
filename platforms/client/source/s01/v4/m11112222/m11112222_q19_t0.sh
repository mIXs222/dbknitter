#!/bin/bash

# Update package lists to fetch latest versions
sudo apt-get update

# Required system dependencies for Python and PIP
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymongo pandas redis direct-redis
