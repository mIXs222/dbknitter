#!/bin/bash

# Update package list
sudo apt-get update

# Install Python and pip (if not already installed)
sudo apt-get install -y python3 python3-pip

# Install MongoDB driver (pymongo) for Python
pip3 install pymongo

# Install pandas - Python Data Analysis Library
pip3 install pandas

# Install direct_redis - a custom Redis client
pip3 install git+https://github.com/pfreixes/direct_redis

# Install msgpack - for Redis data serialization/deserialization
pip3 install msgpack-python

# Make sure the script permissions allow execution
chmod +x install_dependencies.sh
