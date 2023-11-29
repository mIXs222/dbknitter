#!/bin/bash

# Install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis
