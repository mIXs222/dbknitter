#!/bin/bash
# Bash script (setup.sh)

# Update package list
apt-get update

# Install Python and pip, if not already installed
apt install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas direct-redis
