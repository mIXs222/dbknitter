#!/bin/bash

# install_dependencies.sh

# Update package lists
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install the pymongo and pandas packages via pip
pip3 install pymongo pandas redis

# Install DirectRedis dependency (you might need to adapt this line if DirectRedis is obtained differently)
pip3 install git+https://github.com/username/direct_redis.git#egg=direct_redis
