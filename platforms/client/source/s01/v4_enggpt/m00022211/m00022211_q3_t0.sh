#!/bin/bash
# file: setup_dependencies.sh

# Ensure Python 3 and pip are installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas redis direct_redis
