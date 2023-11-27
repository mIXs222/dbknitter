#!/bin/bash

# install_dependencies.sh

# Update package lists
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python libraries
pip3 install pymysql pymongo pandas direct_redis
