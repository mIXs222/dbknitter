#!/bin/bash
# Bash Script: install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python3 if not present
sudo apt-get install -y python3

# Install pip for Python3 if not present
sudo apt-get install -y python3-pip

# Install PyMySQL (MySQL Python connector)
pip3 install pymysql
