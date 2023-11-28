#!/bin/bash

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymongo pandas direct-redis

# Set up DirectRedis
echo "Installing DirectRedis..."

# Clone the repository (Assuming this is necessary due to the custom package)
git clone https://github.com/some-repo/direct_redis.git
cd direct_redis
python3 setup.py install
cd ..

# Clean up the package list (optional)
apt-get clean
