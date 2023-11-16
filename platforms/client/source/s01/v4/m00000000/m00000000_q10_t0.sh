#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python3 and PIP
sudo apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql
