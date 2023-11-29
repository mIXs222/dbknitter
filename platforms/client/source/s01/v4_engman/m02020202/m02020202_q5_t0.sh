#!/bin/bash

# Update package list
apt-get update

# Install pip, a package manager for Python packages, if it's not installed
apt-get install -y python3-pip

# Install Python packages
pip3 install pandas pymysql direct_redis
