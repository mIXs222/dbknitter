#!/bin/bash

# Update package list and install python3-pip if it's not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql

# Run the Python script
python3 query.py
