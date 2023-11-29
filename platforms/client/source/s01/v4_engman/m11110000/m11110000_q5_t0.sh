#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install Python, pip and the necessary client libraries for MySQL and MongoDB
apt-get install -y python3 python3-pip
pip3 install pymysql pymongo

# Note: The above commands may need to be run with sudo, depending on your user permissions
