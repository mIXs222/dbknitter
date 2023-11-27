#!/bin/bash
# Bash script to install dependencies for the Python code

# Update package list
apt-get update

# Install Python 3 and pip if they're not already installed
apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql

# Install PyMongo
pip3 install pymongo
