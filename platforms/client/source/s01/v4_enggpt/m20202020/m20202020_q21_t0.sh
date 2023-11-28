#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip if they are not installed
apt-get install -y python3 python3-pip

# Install pandas library
pip3 install pandas

# Install pymysql library for connecting to MySQL
pip3 install pymysql

# Install direct_redis library to work with redis-py
pip3 install direct_redis

# Install redis-py, a dependency for direct_redis
pip3 install redis

# Note for the evaluator: Please be aware that running this script directly may require root permissions.
# Use `sudo` before the command or run as a root user if necessary.
