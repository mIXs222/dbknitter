#!/bin/bash

# Update the package index
sudo apt-get update

# Install Python 3 and pip if they're not installed already
sudo apt-get install -y python3 python3-pip

# Install MySQL, pymongo, pandas, and direct_redis
pip3 install pymysql pymongo pandas direct-redis
