#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python 3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install direct_redis (This package name is hypothetical, as there is no such known package with exact this name.
# Replace 'direct_redis' with the correct package name if it exists, or install the package based on your source of direct_redis.)
pip3 install direct_redis
