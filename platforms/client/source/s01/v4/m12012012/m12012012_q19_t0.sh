#!/bin/bash
# Bash Script: install_dependencies.sh

# Update the package list
apt-get update

# Install Python3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pandas redis direct-redis

# Optional: Install any other dependencies if needed
