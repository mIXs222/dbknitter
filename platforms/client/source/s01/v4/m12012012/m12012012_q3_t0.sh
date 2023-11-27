#!/bin/bash
# install_dependencies.sh

# Update package list and install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install python packages
pip3 install pandas pymysql pymongo direct-redis
