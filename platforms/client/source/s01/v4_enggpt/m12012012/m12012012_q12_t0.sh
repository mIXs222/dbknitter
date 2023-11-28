#!/bin/bash

# Assuming this script is running in an environment with Python3 and pip installed.

# Update the package index
sudo apt-get update

# Install Python MongoDB client (pymongo)
pip3 install pymongo

# Install DirectRedis, for direct communication with Redis
pip3 install direct_redis

# Install Pandas, required for data manipulation
pip3 install pandas

echo "All dependencies have been installed."
