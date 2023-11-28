#!/bin/bash

# Update package list
apt-get update

# Install Python and pip if they are not already installed
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas

# Download and configure the necessary Redis client for Python ("direct_redis")
pip3 install git+https://github.com/salpreh/direct-redis.git@master#egg=direct_redis
