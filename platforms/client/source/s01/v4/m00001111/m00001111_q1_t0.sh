#!/bin/bash
# Update and install required system packages
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo
