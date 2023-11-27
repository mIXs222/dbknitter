#!/bin/bash

# install_dependencies.sh

# Update packages and install pip if it's not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymongo, pandas and direct_redis
pip3 install pymongo pandas direct-redis

# Ensure the correct version of direct_redis is installed
# Replace '0.0.0' with the actual version required
pip3 install direct-redis==0.0.0
