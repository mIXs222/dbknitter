#!/bin/bash

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install pymongo, direct-redis, and pandas packages
pip3 install pymongo direct_redis pandas
