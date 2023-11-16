#!/bin/bash

# Update package lists
apt-get update

# Install Python if it is not already installed
apt-get install -y python3

# Install pip for Python package management
apt-get install -y python3-pip

# Install pymysql to connect to the MySQL server
pip3 install pymysql

# Install pandas for data manipulation
pip3 install pandas

# Install direct_redis for Redis connection
pip3 install direct_redis

# Install CSV module
pip3 install python-csv
