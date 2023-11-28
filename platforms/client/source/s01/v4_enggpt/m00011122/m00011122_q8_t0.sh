#!/bin/bash

# Update package list
apt-get update

# Install Python and Pip if they are not installed
apt-get install -y python3 python3-pip
pip3 install pymysql pymongo pandas direct_redis

# If the `direct_redis` library does not exist in PyPi,
# then it would need to be installed from source or another repository.
# Please make sure you have appropriate source for the `direct_redis` package.
