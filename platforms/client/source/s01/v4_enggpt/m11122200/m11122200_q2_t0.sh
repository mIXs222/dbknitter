#!/bin/bash

# Update package lists
apt-get update

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install MongoDB driver (pymongo)
pip3 install pymongo

# Install Redis driver (direct_redis)
pip3 install direct_redis

# Install pandas
pip3 install pandas
