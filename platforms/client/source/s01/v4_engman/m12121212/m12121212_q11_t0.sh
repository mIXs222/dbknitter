# File name: setup.sh

#!/usr/bin/env bash

# Updating the package list
apt-get update

# Installing pip and Python dev tools
apt-get install -y python3-pip python3-dev

# Make sure that setuptools is installed
python3 -m pip --no-cache-dir install --upgrade setuptools

# Install pymongo
python3 -m pip --no-cache-dir install pymongo

# Install pandas
python3 -m pip --no-cache-dir install pandas

# Install redis
python3 -m pip --no-cache-dir install redis

# Install direct_redis
python3 -m pip --no-cache-dir install direct-redis

# Note: Since the setup might be running as a script, it would be wise to use a virtual environment if possible. 
# This example does not include setting up a virtual environment to keep things simple.
