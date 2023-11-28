#!/bin/bash

# Update package list and upgrade existing packages
apt-get update
apt-get upgrade -y

# Install pip for Python package management
apt-get install -y python3-pip

# Install Python packages
pip3 install pymysql pymongo pandas direct_redis
