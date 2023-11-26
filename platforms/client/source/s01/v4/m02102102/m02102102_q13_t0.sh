#!/bin/bash
# Bash script to install dependencies (install_dependencies.sh)

# Update the package index
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo
