#!/bin/bash

# install_dependencies.sh

# Update package list
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install Redis server (in case it's needed for local testing)
sudo apt-get install -y redis-server

# Start Redis server (in case it's needed for local testing)
sudo service redis-server start

# Install Python packages
pip3 install pandas redis direct_redis
