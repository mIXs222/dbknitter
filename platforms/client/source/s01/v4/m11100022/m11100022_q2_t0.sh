#!/bin/bash
# File: install_dependencies.sh

# Assuming Python 3 is already installed

# Update package list and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymongo and pymysql
pip3 install pymongo pymysql
