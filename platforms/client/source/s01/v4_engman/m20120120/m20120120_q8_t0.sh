#!/bin/bash
# bash script (install_dependencies.sh)

# Update the package list
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pymongo pandas direct_redis
