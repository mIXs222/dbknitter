#!/bin/bash
# Bash script: install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python pip and Redis if not already installed
sudo apt-get install -y python3-pip redis-server

# Install pymongo and direct_redis using pip
pip3 install pymongo direct_redis pandas
