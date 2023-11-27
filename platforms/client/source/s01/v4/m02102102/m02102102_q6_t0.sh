#!/bin/bash

# Update package list and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pandas redis direct_redis
