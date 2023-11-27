#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install Python library dependencies
pip3 install pymysql pandas redis direct_redis
