#!/bin/bash

# Update system packages and install pip
sudo apt-get update -y
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pandas redis direct-redis
