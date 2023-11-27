#!/bin/bash

# Update packages and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas redis direct_redis
