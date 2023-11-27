#!/bin/bash

# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python 3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install the required libraries
pip3 install pymysql pymongo pandas direct_redis
