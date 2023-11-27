#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install pip if not already installed
sudo apt-get install -y python3-pip

# Install pymysql and pandas using pip
pip3 install pymysql pandas

# Install direct_redis using pip
pip3 install git+https://github.com/Yannael/direct_redis.git
