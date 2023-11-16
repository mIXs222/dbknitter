#!/bin/bash
# Bash script to install dependencies for the Python code

# Update package information
sudo apt-get update

# Install Python 3 and pip (if it's not already installed)
sudo apt-get install -y python3 python3-pip

# Install pymysql via pip to connect to the MySQL database
pip3 install pymysql

# If needed, adjust the permissions of the Python script to make it executable
chmod +x script_name.py
