#!/bin/bash
# Install Python and Redis client dependencies

# Update package list and install python3 and pip
apt-get update
apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql

# Install custom direct_redis (assuming it's a custom library; not found in PyPI)
pip3 install git+https://github.com/your-repo/direct_redis.git

# Install Pandas
pip3 install pandas
