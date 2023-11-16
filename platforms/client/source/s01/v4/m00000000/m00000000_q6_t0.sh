#!/bin/bash
# install_dependencies.sh

# Update package information (optional, uncomment if needed)
# sudo apt-get update

# Install pip for Python3 if it's not installed
sudo apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql

