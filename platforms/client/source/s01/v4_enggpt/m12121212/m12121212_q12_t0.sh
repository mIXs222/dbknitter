#!/bin/bash
# Bash Script: install_dependencies.sh

# Update package list and upgrade existing packages
apt-get update
apt-get upgrade -y

# Install Python and pip
apt-get install -y python3 python3-pip

# Install MongoDB and Redis clients for Python
pip3 install pymongo direct_redis pandas
