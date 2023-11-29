#!/bin/bash
# install_dependencies.sh

# Update repositories and install Python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python MongoDB driver (pymongo)
pip3 install pymongo

# Install Pandas for handling data
pip3 install pandas

# Install direct_redis to handle Redis connections
pip3 install direct-redis

# Install other potentially missing pandas dependencies
pip3 install numpy pytz python-dateutil
