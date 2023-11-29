#!/bin/bash

# Update package list and install python3-pip if not present
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct_redis

echo "All dependencies were installed successfully."
