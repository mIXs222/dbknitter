#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install pip for Python package management
apt-get install -y python3-pip

# Install MongoDB Python Client - pymongo
pip3 install pymongo

# Install direct_redis
pip3 install git+https://github.com/RedisLabsModules/direct_redis.git

# Install pandas
pip3 install pandas
