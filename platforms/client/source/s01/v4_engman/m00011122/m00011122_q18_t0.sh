#!/bin/bash

# Update package list and upgrade
apt-get update

# Install Python and pip (In case they are not installed)
apt-get install -y python3 python3-pip

# Install Python MongoDB client
pip3 install pymongo

# Install direct_redis (Assuming it's available on PyPi; if not, should be obtained in the way provided by its source)
pip3 install direct_redis

# Install pandas
pip3 install pandas
