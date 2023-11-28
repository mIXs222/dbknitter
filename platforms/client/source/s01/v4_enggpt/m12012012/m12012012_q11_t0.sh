#!/bin/bash

# Bash Script: install_dependencies.sh
# Update package list and upgrade existing packages
apt-get update && apt-get upgrade -y

# Install Python3, pip
apt-get install -y python3 python3-pip

# Install pymongo, pandas, and redis-py
pip3 install pymongo pandas redis direct_redis
