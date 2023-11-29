#!/bin/bash

# Update package list
sudo apt update

# Install MongoDB, Redis and Python3-pip
sudo apt install -y mongodb redis-server python3-pip

# Install Python packages
pip3 install pymongo pandas redis direct-redis

# Start MongoDB and Redis services
sudo systemctl start mongodb
sudo systemctl start redis-server

# Note: If `direct_redis` package does not exist or is not suitable, a custom implementation using `redis.Redis` or an alternative package will be required.
