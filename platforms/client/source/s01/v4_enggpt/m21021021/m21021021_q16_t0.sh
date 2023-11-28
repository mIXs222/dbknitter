#!/bin/bash
# Bash script: install_dependencies.sh

# Update package list and install Python pip
sudo apt update
sudo apt install -y python3-pip

# Install required python libraries
pip3 install pandas pymysql pymongo direct-redis
