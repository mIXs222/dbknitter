#!/bin/bash

# Install redis for direct_redis dependency
sudo apt-get install -y redis-server

# Ensure pip is installed
sudo apt-get install -y python3-pip

# Install Pandas
pip3 install pandas

# Install direct_redis
pip3 install git+https://github.com/dpressel/direct-redis.git
