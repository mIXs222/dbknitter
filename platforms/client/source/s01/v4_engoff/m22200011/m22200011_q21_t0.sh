#!/bin/bash
# install_dependencies.sh

# Update package list and upgrade existing packages
sudo apt update && sudo apt upgrade -y

# Install Python3 and pip if they are not installed
sudo apt install python3 python3-pip -y

# Install necessary Python libraries
pip3 install pymysql pymongo pandas direct_redis

# Run the Python script
python3 suppliers_query.py
