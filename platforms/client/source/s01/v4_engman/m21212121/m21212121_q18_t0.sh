#!/bin/bash

# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install MongoDB
sudo apt-get install -y mongodb

# Start the MongoDB service
sudo service mongodb start

# Install Redis
sudo apt-get install -y redis-server

# Start the Redis service
sudo service redis-server start

# Install required Python packages
pip3 install pymongo direct_redis pandas
