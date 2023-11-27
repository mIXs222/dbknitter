#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip if not already installed
apt-get install -y python3 python3-pip

# Install pymysql, pymongo, pandas and direct_redis using pip
pip3 install pymysql pymongo pandas direct_redis
