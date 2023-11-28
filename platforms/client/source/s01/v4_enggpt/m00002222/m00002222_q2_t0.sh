#!/bin/bash

# Install Python and Pip if not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pandas pymysql sqlalchemy

# Redis doesn't need to be installed if using `direct_redis.DirectRedis` as it seems specific.
# If `direct_redis` is a Python package, it should be included here for installation.
# Please verify whether `direct_redis` is available for installation via pip.
# Replace 'direct_redis' with the actual package name if different.
pip3 install direct_redis
