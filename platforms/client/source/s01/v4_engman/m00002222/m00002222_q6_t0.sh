#!/bin/bash

# Update package list
apt-get update

# Install Python 3 and pip if they are not installed
apt-get install -y python3
apt-get install -y python3-pip

# Install Pandas using pip
pip3 install pandas

# Install direct_redis package using pip
pip3 install direct_redis

# Install additional dependency if necessary
# For example, if your system does not have Redis server installed and you want to test locally
# apt-get install -y redis-server
# Note: This is not necessary if you're connecting to an external Redis server as described by the hostname 'redis'
