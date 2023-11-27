#!/bin/bash
# install_dependencies.sh

# Update package lists
apt-get update

# Install MongoDB
apt-get install -y mongodb

# Install Redis
apt-get install -y redis-server

# Install Python3, pip, and other essentials
apt-get install -y python3 python3-pip

# Install Python packages
pip3 install pymongo pandas direct_redis
