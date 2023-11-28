#!/bin/bash

# Update package lists
apt-get update

# Install Python 3 and pip
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas msgpack-python direct-redis
