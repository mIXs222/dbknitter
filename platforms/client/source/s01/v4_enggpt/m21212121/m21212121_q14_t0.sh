#!/bin/bash

# install_dependencies.sh

# Update package manager
sudo apt-get update

# Install Python pip and Redis
sudo apt-get install -y python3-pip redis-server

# Install required Python packages
pip3 install pymongo direct-redis pandas

# Start the Redis server
sudo service redis-server start
