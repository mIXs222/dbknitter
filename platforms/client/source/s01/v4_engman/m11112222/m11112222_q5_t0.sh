#!/bin/bash
# Bash script to install necessary dependencies for the python script

# Update the package lists
sudo apt-get update

# Install MongoDB dependencies
sudo apt-get install -y mongodb

# Install Redis dependencies
sudo apt-get install -y redis-server

# Install Python 3 and pip if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymongo pandas 'redis[aioredis,pandas]' direct_redis

# Make sure MongoDB and Redis services are running
sudo systemctl start mongodb
sudo systemctl enable mongodb
sudo systemctl start redis-server
sudo systemctl enable redis-server
