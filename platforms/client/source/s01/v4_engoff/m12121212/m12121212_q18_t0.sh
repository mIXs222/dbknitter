#!/bin/bash

# Update package list
apt-get update

# Install pip for Python3
apt-get install -y python3-pip

# Install pymongo, redis, pandas and direct_redis using pip
pip3 install pymongo direct_redis redis pandas
