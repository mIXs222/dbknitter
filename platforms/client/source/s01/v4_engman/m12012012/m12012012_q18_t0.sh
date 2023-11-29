#!/bin/bash

# Update package list
apt-get update

# Install MySQL dependencies
apt-get install -y python3-pymysql

# Install MongoDB dependencies
apt-get install -y python3-pymongo

# Install Redis dependencies
pip install direct-redis

# Install Pandas
pip install pandas
