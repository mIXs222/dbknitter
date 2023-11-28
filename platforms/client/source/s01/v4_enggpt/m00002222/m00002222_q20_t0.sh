#!/bin/bash
# Install Python dependencies

# Updating the package list and installing pip for Python3
apt-get update
apt-get install -y python3-pip

# Upgrading pip to its latest version
pip3 install --upgrade pip

# Installing the pymysql package for MySQL connectivity
pip3 install pymysql

# Installing pandas for data manipulation
pip3 install pandas

# Install direct_redis as the specific Redis client
pip3 install direct-redis

# Note: The actual package names for apt-get might differ depending on the Linux distribution and version.
