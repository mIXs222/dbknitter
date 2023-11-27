#!/bin/bash

# Update package list and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymongo and redis-py
pip3 install pymongo pandas redis direct_redis
