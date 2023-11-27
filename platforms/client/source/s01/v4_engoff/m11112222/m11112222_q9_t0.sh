#!/bin/bash

# Update and install system dependencies
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install pymongo pandas redis direct-redis
