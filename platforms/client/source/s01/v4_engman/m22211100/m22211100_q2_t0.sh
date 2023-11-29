#!/bin/bash

# Update the package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install mongo
apt-get install -y mongodb

# Install redis
apt-get install -y redis-server

# Using pip to install the required Python libraries
pip3 install pymongo pandas redis direct_redis
