#!/bin/bash
# install_dependencies.sh

# Update the package list
sudo apt-get update

# Install Python and pip if they're not already installed
sudo apt-get install -y python3 python3-pip

# Install Python dependencies for MySQL and MongoDB
pip3 install pymysql pymongo
