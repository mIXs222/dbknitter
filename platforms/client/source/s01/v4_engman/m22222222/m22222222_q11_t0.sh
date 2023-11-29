#!/bin/bash
# This script is used to install dependencies required by important_stock_query.py

# Update package list
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pandas library
sudo pip3 install pandas

# Install direct_redis, assuming it is a package that needs to be installed from a location like GitHub
# This is a placeholder since "direct_redis" isn't an actual known python package at the moment
# In a real-world scenario, one would replace the URL with the actual repository URL or PyPi package
# e.g. git+https://github.com/username/direct_redis.git or direct-redis
sudo pip3 install git+https://github.com/path_to/direct_redis_repository.git

# If the package is already hosted on PyPi, uncomment the following line:
# sudo pip3 install direct_redis
