#!/bin/bash

# install_dependencies.sh
# This script will update package lists, install Python 3, pip and the pymysql library

# Update package lists
apt-get update

# Install Python 3 and pip
apt-get install -y python3 python3-pip

# Install pymysql library
pip3 install pymysql
