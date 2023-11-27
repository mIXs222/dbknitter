#!/bin/bash

# install_dependencies.sh
# Update package list and install Python with pip
apt-get update
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas pymysql direct_redis
