#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip and Redis if they are not already installed
sudo apt-get install -y python3-pip redis-server

# Start the Redis server
sudo systemctl start redis-server

# Install Python dependencies
pip3 install pymysql pandas direct_redis
