#!/bin/bash

# Update package list
sudo apt-get update

# Install Python and Python package manager (pip)
sudo apt-get install -y python3 python3-pip

# Install pymongo, pandas, and redis-py
pip3 install pymongo pandas redis direct-redis

# Note: Instructions assume queryset is in 'query.py' and script is in 'install_dependencies.sh'
# Make script executable with 'chmod +x install_dependencies.sh'
# Run the script with './install_dependencies.sh'

