#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install Python, pip and necessary build tools
apt-get install -y python3 python3-pip python3-dev build-essential

# Install the necessary Python packages
pip3 install pandas pymysql pymongo direct-redis
