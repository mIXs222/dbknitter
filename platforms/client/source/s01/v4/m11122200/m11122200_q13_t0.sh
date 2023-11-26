#!/bin/sh

# Update package index
apt-get update

# Install Python 3 and pip
apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymysql pandas direct-redis
