#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python 3 and pip if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install MySQL and MongoDB Python drivers
python3 -m pip install pymysql
python3 -m pip install pymongo
