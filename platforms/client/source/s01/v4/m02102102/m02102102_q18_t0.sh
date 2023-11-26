#!/bin/bash
# Bash script: install_dependencies.sh

# Update the package index
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install MySQL client
apt-get install -y default-libmysqlclient-dev

# Install dependencies using pip
pip3 install pymysql pymongo pandas redis direct_redis
