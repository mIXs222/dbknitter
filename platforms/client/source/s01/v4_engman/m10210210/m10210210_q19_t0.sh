#!/bin/bash
# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python3, pip and Redis if they are not installed
sudo apt-get install -y python3 python3-pip redis-server

# Install required Python packages
pip3 install pymysql pandas direct_redis
