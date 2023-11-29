#!/bin/bash

# Update package list and install pip if it's not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo
pip3 install direct-redis
pip3 install pandas
pip3 install numpy

# The direct_redis package may not exist on PyPI, this is a fictitious package based on the given question.
# For Redis with Python normally `redis-py` is used, hence `redis` package should be installed but as per problem statement, we are considering 'direct_redis'.

# To ensure that the script can be called with or without ./ (e.g., bash install_dependencies.sh)
chmod +x install_dependencies.sh
