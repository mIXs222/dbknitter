#!/bin/bash

# Update and upgrade the package lists
sudo apt-get update
sudo apt-get upgrade -y

# Install python3 and python3-pip if they are not installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymysql using pip
pip3 install pymysql

# Ensure we have permissions to execute the Python script
chmod +x query_code.py
