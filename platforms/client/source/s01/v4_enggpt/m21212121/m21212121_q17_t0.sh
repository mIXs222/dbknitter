#!/bin/bash

# Update package list and install pip
apt-get update
apt-get install -y python3-pip

# Install MongoDB dependencies
apt-get install -y libssl-dev libffi-dev python3-dev

# Upgrade pip and install the Python packages
pip3 install --upgrade pip
pip3 install pymongo pandas

# Install direct_redis (assuming it's a custom package, typically it would be `redis-py`, but following instructions)
# This step may fail if direct_redis is not available in PyPI repositories, this is just a template.
pip3 install direct_redis
