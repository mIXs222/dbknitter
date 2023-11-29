#!/bin/bash

# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install pip for Python package management
sudo apt-get install -y python3-pip

# Install Python MongoDB client (pymongo)
pip3 install pymongo

# Install pandas, direct_redis, and necessary dependency for reading Redis msgpack
pip3 install pandas direct_redis msgpack-python
