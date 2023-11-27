#!/bin/bash
# install_dependencies.sh

# Update package lists for upgrades for packages that need upgrading
apt-get update

# Install python3 and pip if they are not already installed
apt-get install -y python3 python3-pip

# Install Python packages
pip3 install pymysql pymongo pandas redis direct_redis
