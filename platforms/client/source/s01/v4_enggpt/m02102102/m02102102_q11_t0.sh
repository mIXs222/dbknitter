#!/bin/bash
# Bash script to install dependencies (setup.sh)

# Update package lists
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install the pymysql and direct_redis package
pip3 install pymysql pandas 'direct_redis @ git+https://github.com/dzhuo/direct_redis-py'
