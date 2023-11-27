#!/bin/bash

# Update package list
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install MongoDB driver: pymongo
pip3 install pymongo

# Install Pandas for data manipulation
pip3 install pandas

# Install direct_redis package
pip3 install git+https://github.com/pinnacle0/direct-redis#egg=direct-redis

# Set necessary environment variables if needed
export REDIS_HOST=redis
export REDIS_PORT=6379
export MONGODB_HOST=mongodb
export MONGODB_PORT=27017
