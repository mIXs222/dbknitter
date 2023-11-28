#!/bin/bash
# setup.sh

# Update package list and upgrade existing packages
apt-get update
apt-get -y upgrade

# Install python3 and pip
apt-get install -y python3
apt-get install -y python3-pip

# Upgrade pip and install required Python packages
pip3 install --upgrade pip
pip3 install pymysql
