#!/bin/bash
# install_dependencies.sh

# Update package lists
apt-get update

# Install Python 3 and Pip if they are not already installed
apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pandas direct_redis
