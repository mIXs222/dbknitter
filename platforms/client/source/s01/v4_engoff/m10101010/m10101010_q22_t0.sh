#!/bin/bash

# Bash Script: install_dependencies.sh

# Update the package lists
apt-get update

# Install Python3 and the pip package manager
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo
