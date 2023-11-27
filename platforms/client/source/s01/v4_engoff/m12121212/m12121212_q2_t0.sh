#!/bin/bash

# Update package lists
apt-get update

# Install MongoDB
apt-get install -y mongodb

# Install Redis
apt-get install -y redis-server

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymongo redis direct-redis pandas
