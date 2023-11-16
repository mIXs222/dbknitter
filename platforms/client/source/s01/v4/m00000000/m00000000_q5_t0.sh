#!/bin/bash
# setup.sh

# Update your package index
apt-get update -y

# Install Python3 and pip (if not already installed)
apt-get install python3 -y
apt-get install python3-pip -y

# Install pymysql Python library
pip3 install pymysql
