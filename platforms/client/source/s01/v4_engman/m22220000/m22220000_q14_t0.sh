#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pandas redis direct_redis
