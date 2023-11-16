#!/bin/bash
# install_dependencies.sh

# Update package list and install pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pymongo
