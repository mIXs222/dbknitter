#!/bin/bash

# Update package list
sudo apt-get update

# Install python pip and redis
sudo apt-get install -y python3-pip redis-server

# Install Python dependencies
pip3 install pymongo pandas redis direct_redis
