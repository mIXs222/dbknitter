#!/bin/bash
# install_dependencies.sh

# Update and install pip if it is not already installed
sudo apt-get update -y
sudo apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql

# Run the Python script
python3 query.py
