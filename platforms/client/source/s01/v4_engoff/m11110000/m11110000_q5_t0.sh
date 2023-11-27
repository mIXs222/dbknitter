#!/bin/bash
# This script installs all dependencies needed to run the Python code

# Update package manager and install Python 3 and pip if they are not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install PyMySQL and PyMongo using pip
pip3 install pymysql pymongo
