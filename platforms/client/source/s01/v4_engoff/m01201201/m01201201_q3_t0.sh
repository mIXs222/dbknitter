#!/bin/bash
# Bash script (install_dependencies.sh)

# Update package list
apt-get update

# Install Python3 and pip if not already installed
apt-get install -y python3 python3-pip

# Install pymysql, pymongo, pandas, and direct_redis libraries
pip3 install pymysql pymongo pandas direct_redis
