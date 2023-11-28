#!/bin/bash
set -e

# Update package lists
sudo apt-get update

# Install pip and Python dev tools if not already installed
sudo apt-get install -y python3-pip python3-dev

# Install Redis
sudo apt-get install -y redis-server

# Start and enable Redis server
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Install the required Python packages
pip3 install pandas direct-redis
