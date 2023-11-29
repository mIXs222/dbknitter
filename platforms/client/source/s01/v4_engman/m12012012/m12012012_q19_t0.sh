#!/bin/bash
# install_dependencies.sh

# Update package list and install Python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python library dependencies
pip3 install pymysql pandas direct_redis

# Note: Additional system dependencies might be required. This script assumes that Python3 and pip are not installed.
