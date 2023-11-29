#!/bin/bash
# Bash script to install Python dependencies required to run the Python code.

# Update package lists
sudo apt-get update

# Install Python3 and pip if not installed
sudo apt-get install -y python3 python3-pip

# Install Python libraries for MySQL and MongoDB
pip3 install pymysql pymongo
