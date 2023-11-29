#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python pip if it's not already installed
sudo apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql

# Install PyMongo
pip3 install pymongo

# Install required libraries for CSV and datetime, they should be available in Python's standard library.
