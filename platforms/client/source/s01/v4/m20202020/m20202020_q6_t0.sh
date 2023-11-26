#!/bin/bash

# install.sh
# Make sure to run this script with root privileges

# Update package lists
apt-get update

# Install Python 3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install pymysql via pip
pip3 install pymysql
