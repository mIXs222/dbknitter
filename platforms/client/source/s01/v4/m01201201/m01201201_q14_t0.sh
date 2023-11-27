#!/bin/bash

# Update Repositories
apt-get update

# Install MongoDB dependencies
apt-get install -y mongodb-clients

# Install Python3 and pip
apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymongo pandas direct_redis
