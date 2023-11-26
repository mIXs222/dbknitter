#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install MongoDB driver
pip3 install pymongo

# Install direct_redis (assuming it's available on PyPI,
# otherwise, you will need to provide more information)
pip3 install direct_redis

# Install pandas
pip3 install pandas
