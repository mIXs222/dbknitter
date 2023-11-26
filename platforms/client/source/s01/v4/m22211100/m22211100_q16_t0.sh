#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip if not installed
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas redis

# Install DirectRedis if not available
if ! python3 -c "import direct_redis" > /dev/null 2>&1; then
    # Assuming direct_redis package is available on PyPI or a similar index
    pip3 install direct_redis
fi
