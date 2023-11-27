#!/bin/bash

# Update package lists
apt-get update

# Install MongoDB, Redis and direct_redis dependencies
apt-get install -y mongodb redis-server python3 python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis direct-redis
