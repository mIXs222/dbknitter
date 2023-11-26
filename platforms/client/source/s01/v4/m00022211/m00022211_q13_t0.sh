#!/bin/bash
# Ubuntu/Debian system assumed
set -e

# Update system and install pip
sudo apt update
sudo apt install -y python3-pip

# Install required Python packages
pip3 install pymongo direct-redis pandas
