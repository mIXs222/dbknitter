#!/bin/bash
# install_dependencies.sh

# Install Python and pip if they are not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pymysql pymongo pandas direct_redis
