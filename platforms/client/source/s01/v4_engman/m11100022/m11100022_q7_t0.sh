#!/bin/bash
# install_dependencies.sh

# Update package lists
apt-get update

# Install Python 3 and PIP if not already installed
apt-get install -y python3 python3-pip

# Install Python library dependencies
pip3 install pymysql pymongo pandas direct-redis
