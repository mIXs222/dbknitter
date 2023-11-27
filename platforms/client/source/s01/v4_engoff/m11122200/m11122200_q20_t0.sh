#!/bin/bash

# Update package list and install Python3 and pip if not installed
sudo apt update
sudo apt -y install python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas redis direct_redis

echo "All dependencies have been installed."
