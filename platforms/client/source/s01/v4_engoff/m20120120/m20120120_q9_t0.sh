#!/bin/bash

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pymongo pandas direct_redis

# Note: You might need sudo or adjust the package manager (apt-get) if you are not on a Debian-based system.
# Also, ensure that you have direct access to install packages on your system.

