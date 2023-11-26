#!/bin/bash

# Update and install system dependencies
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymysql and pandas using pip
pip3 install pymysql pandas

# Installing direct_redis will require fetching it from its source, as it's not a standard package.
# Replace `git+https://github.com/foo/direct_redis.git#egg=direct_redis` with the actual repository URL.
pip3 install git+https://github.com/foo/direct_redis.git#egg=direct_redis
