#!/bin/bash

# Update the package lists
apt-get update

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install pymysql, pymongo, pandas, and direct_redis via pip
pip3 install pymysql pymongo pandas direct-redis
