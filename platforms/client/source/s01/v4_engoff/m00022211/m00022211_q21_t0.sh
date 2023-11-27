#!/bin/bash
# Bash file: install_dependencies.sh

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pymysql pymongo pandas direct_redis
