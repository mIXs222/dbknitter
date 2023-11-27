#!/bin/bash

# install_dependencies.sh

# Update package list and install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pymongo redis pandas

# If direct_redis is not directly available, you might need to install from source or use an equivalent Redis client for pandas
# Assuming direct_redis is available through pip for this script, although it's not a standard package available on PyPI

pip3 install direct_redis

# Run the Python script to execute the query
python3 global_sales_opportunity_query.py
