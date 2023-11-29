#!/bin/bash

# Update package list
sudo apt-get update

# Install Python and pip if they are not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install Redis and Direct Redis wrapper for Python
sudo apt-get install -y redis-server
pip3 install pandas redis direct-redis

# Make sure the Redis server is running

