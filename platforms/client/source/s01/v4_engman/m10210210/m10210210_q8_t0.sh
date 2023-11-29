#!/bin/bash

# install_dependencies.sh

# Update the package list
sudo apt-get update

# Upgrade existing packages
sudo apt-get upgrade -y

# Install Python and pip if they are not already installed
sudo apt-get install python3 python3-pip -y

# Install required Python libraries
pip3 install pymysql pymongo pandas redis direct-redis

# Run the Python script
# python3 multi_db_query.py
