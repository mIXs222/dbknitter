#!/bin/bash

# Update system package index
sudo apt-get update

# Install pip and Python development files
sudo apt-get install -y python3-pip python3-dev

# Install MongoDB driver and Redis library
pip3 install pymongo pandas direct_redis
