#!/bin/bash
# install_dependencies.sh

# Update the package list
sudo apt-get update

# Install Python 3 and pip3 if they are not installed
sudo apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql
