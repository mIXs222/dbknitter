#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install MongoDB driver: pymongo
pip3 install pymongo

# Install Redis driver: redis-py and direct_redis
pip3 install redis direct_redis

# Install pandas for data manipulation
pip3 install pandas

# Install msgpack for serialization in Redis
pip3 install msgpack-python
