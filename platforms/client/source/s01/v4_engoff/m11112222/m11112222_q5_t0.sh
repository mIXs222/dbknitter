#!/bin/bash

# Update package list
sudo apt update

# Install Python3 and pip if not already installed
sudo apt install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas direct-redis

# Note: If `direct_redis.DirectRedis` does not work as expected in the environment,
# it might be necessary to modify the import or installation process,
# as `direct_redis.DirectRedis` doesn't seem to be a standard library or known package.
