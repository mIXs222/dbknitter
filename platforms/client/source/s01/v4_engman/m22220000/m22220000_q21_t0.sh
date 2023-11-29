#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install pymysql for MySQL
pip install pymysql

# Install direct_redis for Redis, assuming it's a Python package
pip install direct_redis

# Install pandas for data manipulation
pip install pandas
