#!/bin/bash
# install.sh

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install the pymysql package for Python
pip3 install pymysql
