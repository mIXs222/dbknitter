#!/bin/bash
# install_dependencies.sh

# Install Python pip if not already installed
apt-get update && apt-get install -y python3-pip

# Install the required Python packages
pip3 install pandas direct-redis
